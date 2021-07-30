open! Core_kernel
open! Async_kernel

module Server = struct
  type addr = Luv.Sockaddr.t

  let sexp_of_addr addr = Option.sexp_of_t String.sexp_of_t (Luv.Sockaddr.to_string addr)

  module Connection = struct
    type t = { client_stream : ([ `TCP ] Luv.Stream.t[@sexp.opaque]) }
    [@@deriving sexp_of]

    let close t =
      Deferred.create (fun ivar ->
          Luv.Handle.close t.client_stream (fun () -> Ivar.fill ivar ()))
    ;;
  end

  type t =
    { address : addr
    ; socket_stream : ([ `TCP ] Luv.Stream.t[@sexp.opaque])
    ; connections : Connection.t Bag.t
    ; handle_client : [ `TCP ] Luv.Stream.t -> unit Deferred.t
    ; on_handler_error : exn -> unit
    }
  [@@deriving sexp_of]

  let total_connections t = Bag.length t.connections
  let is_closed t = (Fn.non Luv.Handle.is_active) t.socket_stream

  let close t =
    Deferred.create (fun ivar ->
        Luv.Handle.close t.socket_stream (fun () -> Ivar.fill ivar ()))
  ;;

  let rec accept t =
    if not (is_closed t)
    then (
      let client =
        match Luv.TCP.init () with
        | Ok client_stream -> client_stream
        | Error e ->
          Error.raise_s
            [%message "Could not initialize client socket" ~error:(Luv.Error.strerror e)]
      in
      match Luv.Stream.accept ~server:t.socket_stream ~client with
      | Error _ -> Luv.Handle.close client ignore
      | Ok () ->
        let conn = { Connection.client_stream = client } in
        let ident = Bag.add t.connections conn in
        Monitor.try_with (fun () -> t.handle_client client)
        >>> fun res ->
        Connection.close conn
        >>> fun () ->
        Bag.remove t.connections ident;
        (match res with
        | Ok () -> ()
        | Error e ->
          (try t.on_handler_error e with
          | e ->
            don't_wait_for (close t);
            raise e));
        accept t)
  ;;

  let create ?(backlog = 128) ~handle_client ~on_handler_error address =
    let server =
      let open Result.Let_syntax in
      let%bind server = Luv.TCP.init () in
      let%map () = Luv.TCP.bind server address in
      server
    in
    match server with
    | Ok server ->
      let t =
        { socket_stream = server
        ; address
        ; connections = Bag.create ()
        ; handle_client
        ; on_handler_error
        }
      in
      Luv.Stream.listen ~backlog server (function
          | Error e ->
            let msg = Bigstring.of_string (sprintf "%s\n" (Luv.Error.strerror e)) in
            don't_wait_for (Fd.write Fd.stderr msg |> Deferred.ignore_m)
          | Ok () -> accept t);
      t
    | Error e ->
      Error.raise_s [%message "Could not establish server" ~error:(Luv.Error.strerror e)]
  ;;
end
