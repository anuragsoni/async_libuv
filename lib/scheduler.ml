open Core_kernel
include Async_kernel
include Async_kernel_scheduler
module Kernel_scheduler = Async_kernel_scheduler.Private

(* TODO: Add an api that allows for scheduling all libuv interactions through this
   scheduler module. Most functions in [luv] accept a [?loop] parameter, and the scheduler
   is the only location with knowledge of the current active loop. *)

type t =
  { mutable is_running : bool
  ; kernel_scheduler : Kernel_scheduler.t
  ; initialized_at : (Backtrace.t[@sexp.opaque])
  ; uv_loop : (Luv.Loop.t[@sexp.opaque])
  }
[@@deriving sexp_of]

let create () =
  let kernel_scheduler = Kernel_scheduler.t () in
  (* TODO: Allow using a user-provided libuv loop. *)
  let uv_loop = Luv.Loop.default () in
  { is_running = false; kernel_scheduler; uv_loop; initialized_at = Backtrace.get () }
;;

let advance_scheduler t =
  if Kernel_scheduler.can_run_a_job t.kernel_scheduler
  then Kernel_scheduler.run_cycle t.kernel_scheduler
;;

let cycle t =
  advance_scheduler t;
  if Kernel_scheduler.has_upcoming_event t.kernel_scheduler
  then ignore (Luv.Loop.run ~loop:t.uv_loop ~mode:`NOWAIT () : bool)
  else
    (* TODO: Maybe we should always use `NOWAIT since we are attempting to get
       async_kernel's scheduler and libuv's scheduler to play nice? *)
    ignore (Luv.Loop.run ~loop:t.uv_loop ~mode:`ONCE () : bool);
  advance_scheduler t
;;

let run () =
  let t = create () in
  let rec loop () =
    match Kernel_scheduler.uncaught_exn t.kernel_scheduler with
    | Some error -> error
    | None ->
      cycle t;
      loop ()
  in
  let error =
    try loop () with
    | exn -> Error.create "scheduler buf" (exn, t) [%sexp_of: exn * t]
  in
  (try Caml.do_at_exit () with
  | _ -> ());
  eprintf !"%{sexp: Error.t}" error;
  Caml.exit 1
;;
