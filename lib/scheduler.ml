open Core_kernel
include Async_kernel
include Async_kernel_scheduler
module Kernel_scheduler = Async_kernel_scheduler.Private

type t =
  { mutable is_running : bool
  ; kernel_scheduler : Kernel_scheduler.t
  ; initialized_at : (Backtrace.t[@sexp.opaque])
  ; uv_loop : (Luv.Loop.t[@sexp.opaque])
  }
[@@deriving sexp_of]

let create () =
  let kernel_scheduler = Kernel_scheduler.t () in
  let uv_loop = Luv.Loop.default () in
  { is_running = false; kernel_scheduler; uv_loop; initialized_at = Backtrace.get () }
;;

let cycle t =
  if not (Kernel_scheduler.can_run_a_job t.kernel_scheduler)
  then ignore (Luv.Loop.run ~loop:t.uv_loop ~mode:`NOWAIT () : bool);
  Kernel_scheduler.run_cycle t.kernel_scheduler
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
