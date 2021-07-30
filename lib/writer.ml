open! Core_kernel
open! Async_kernel

module Config = struct
  (* Same as the default value of [buffer_age_limit] for [Async_unix.Writer] *)
  let default_write_timeout = Time_ns.Span.of_min 2.
  let default_max_buffer_size = Int.max_value
  let default_initial_buffer_size = 64 * 1024

  type t =
    { initial_buffer_size : int
    ; max_buffer_size : int
    ; write_timeout : Time_ns.Span.t
    }
  [@@deriving sexp_of]

  let validate t =
    if t.initial_buffer_size <= 0
       || t.initial_buffer_size > t.max_buffer_size
       || Time_ns.Span.( <= ) t.write_timeout Time_ns.Span.zero
    then raise_s [%sexp "Shuttle.Config.validate: invalid config", { t : t }];
    t
  ;;

  let create
      ?(initial_buffer_size = default_initial_buffer_size)
      ?(max_buffer_size = default_max_buffer_size)
      ?(write_timeout = default_write_timeout)
      ()
    =
    validate { initial_buffer_size; max_buffer_size; write_timeout }
  ;;
end

type t =
  { fd : ([ `TCP ] Luv.Stream.t[@sexp.opaque])
  ; config : Config.t
  ; mutable buf : (Faraday.t[@sexp.opaque])
  ; monitor : Monitor.t
  ; mutable close_state : [ `Open | `Start_close | `Closed ]
  ; close_started : unit Ivar.t
  ; close_finished : unit Ivar.t
  ; mutable writer_state : [ `Active | `Stopped | `Inactive ]
  }
[@@deriving sexp_of, fields]

let create ?initial_buffer_size ?max_buffer_size ?write_timeout fd =
  let config = Config.create ?initial_buffer_size ?max_buffer_size ?write_timeout () in
  { fd
  ; config
  ; writer_state = `Inactive
  ; buf = Faraday.create config.initial_buffer_size
  ; monitor = Monitor.create ()
  ; close_state = `Open
  ; close_started = Ivar.create ()
  ; close_finished = Ivar.create ()
  }
;;

let is_closed t =
  match t.close_state with
  | `Open -> false
  | `Closed | `Start_close -> true
;;

let close_started t = Ivar.read t.close_started
let close_finished t = Ivar.read t.close_finished
let is_open = Fn.non is_closed

let mk_iovecs iovecs =
  List.map iovecs ~f:(fun { Faraday.buffer; off; len } ->
      Luv.Buffer.sub buffer ~offset:off ~length:len)
;;

let write_iovecs t iovecs =
  match Luv.Stream.try_write t.fd iovecs with
  | Ok n ->
    Faraday.shift t.buf n;
    `Ok
  | Error (`EAGAIN | `EINTR) -> `Ok
  | Error (`EPIPE | `ECONNRESET | `EHOSTUNREACH | `ENETDOWN | `ENETUNREACH | `ETIMEDOUT)
    -> `Eof
  | Error e ->
    Error.raise_s [%message "Error while writing" ~error:(Luv.Error.strerror e)]
;;

let wait_and_write_iovecs t =
  let ivar = Ivar.create () in
  (match Faraday.operation t.buf with
  | `Yield -> Ivar.fill ivar `Ok
  | `Close -> Ivar.fill ivar `Eof
  | `Writev iovecs ->
    let iovecs = mk_iovecs iovecs in
    Luv.Stream.write t.fd iovecs (fun res count ->
        match res with
        | Ok () ->
          Faraday.shift t.buf count;
          Ivar.fill ivar `Ok
        | Error
            (`EPIPE | `ECONNRESET | `EHOSTUNREACH | `ENETDOWN | `ENETUNREACH | `ETIMEDOUT)
          -> Ivar.fill ivar `Eof
        | Error e -> Ivar.fill ivar (`Error e)));
  Ivar.read ivar
;;

let flushed t f = Faraday.flush t.buf f

let close_stream t =
  Deferred.create (fun ivar -> Luv.Handle.close t (fun () -> Ivar.fill ivar ()))
;;

let close t =
  (match t.close_state with
  | `Closed | `Start_close -> ()
  | `Open ->
    t.close_state <- `Start_close;
    Ivar.fill t.close_started ();
    Deferred.any_unit
      [ after (Time_ns.Span.of_sec 5.)
      ; Deferred.create (fun ivar -> flushed t (fun () -> Ivar.fill ivar ()))
      ]
    >>> fun () ->
    t.close_state <- `Closed;
    close_stream t.fd >>> fun () -> Ivar.fill t.close_finished ());
  close_finished t
;;

let stop_writer t =
  t.writer_state <- `Stopped;
  ignore (Faraday.drain t.buf : int)
;;

module Single_write_result = struct
  type t =
    | Continue
    | Stop
end

let single_write t =
  match Faraday.operation t.buf with
  | `Yield -> Single_write_result.Continue
  | `Close -> Stop
  | `Writev iovecs ->
    (match write_iovecs t (mk_iovecs iovecs) with
    | `Ok -> Continue
    | `Eof -> Stop)
;;

let rec write_everything t =
  match single_write t with
  | Stop -> stop_writer t
  | Continue ->
    if not (Faraday.has_pending_output t.buf)
    then (
      t.writer_state <- `Inactive;
      if is_closed t then stop_writer t)
    else wait_and_write_everything t

and wait_and_write_everything t =
  Clock_ns.with_timeout t.config.write_timeout (wait_and_write_iovecs t)
  >>> fun result ->
  match result with
  | `Result `Ok -> write_everything t
  | `Result `Eof -> stop_writer t
  | `Result (`Error e) ->
    Error.raise_s [%message "Error while writing" ~error:(Luv.Error.strerror e)]
  | `Timeout -> stop_writer t
  | `Result ((`Bad_fd | `Closed) as result) ->
    raise_s
      [%sexp
        "Async_transport.Writer: fd changed"
        , { t : t; ready_to_result = (result : [ `Bad_fd | `Closed ]) }]
;;

let is_writing t =
  match t.writer_state with
  | `Active -> true
  | `Inactive -> false
  | `Stopped -> false
;;

let flush t =
  if (not (is_writing t)) && Faraday.has_pending_output t.buf
  then (
    t.writer_state <- `Active;
    Scheduler.within ~monitor:t.monitor (fun () -> write_everything t))
;;

let ensure_can_write t =
  match t.writer_state with
  | `Inactive | `Active -> ()
  | `Stopped -> raise_s [%sexp "Attempting to write to a closed writer", { t : t }]
;;

let schedule_bigstring t ?pos ?len buf =
  ensure_can_write t;
  Faraday.schedule_bigstring t.buf buf ?off:pos ?len
;;

let write_string t ?pos ?len buf =
  ensure_can_write t;
  Faraday.write_string t.buf buf ?off:pos ?len
;;
