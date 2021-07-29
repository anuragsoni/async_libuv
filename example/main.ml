open! Core_kernel
open! Async_kernel
open! Async_libuv

let run name =
  Fd.with_file name [ `RDONLY ] ~f:(fun fd ->
      let buffer = Bigstring.create 1024 in
      let rec loop () =
        match%bind Fd.read_into fd buffer with
        | `Ok n ->
          let%bind () =
            Deferred.ignore_m (Fd.write Fd.stdout (Bigstring.sub buffer ~pos:0 ~len:n))
          in
          loop ()
        | `Eof -> return ()
      in
      loop ())
;;

let () =
  let name = Sys.argv.(1) in
  don't_wait_for (run name >>= fun () -> Caml.exit 0);
  never_returns (Scheduler.run ())
;;
