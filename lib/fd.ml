open Core_kernel
open Async_kernel

type t = (Luv.File.t[@sexp.opaque]) [@@deriving sexp_of]

let stdin = Luv.File.stdin
let stdout = Luv.File.stdout
let stderr = Luv.File.stderr

let openfile name flags =
  Deferred.create (fun ivar ->
      Luv.File.open_ name flags (function
          | Ok t -> Ivar.fill ivar t
          | Error e ->
            Error.raise_s [%message "Could not open fd" ~error:(Luv.Error.strerror e)]))
;;

let close fd =
  Deferred.create (fun ivar ->
      Luv.File.close fd (function
          | Ok () -> Ivar.fill ivar ()
          | Error e ->
            Error.raise_s [%message "Could not close fd" ~error:(Luv.Error.strerror e)]))
;;

let with_file name flags ~f =
  let%bind fd = openfile name flags in
  Monitor.protect (fun () -> f fd) ~finally:(fun () -> close fd)
;;

let read_into t buffer =
  Deferred.create (fun ivar ->
      Luv.File.read t [ buffer ] (function
          | Ok n ->
            let count = Unsigned.Size_t.to_int n in
            if count = 0 then Ivar.fill ivar `Eof else Ivar.fill ivar (`Ok count)
          | Error e ->
            Error.raise_s
              [%message "Could not read from fd" ~error:(Luv.Error.strerror e)]))
;;

let write t buffer =
  Deferred.create (fun ivar ->
      Luv.File.write t [ buffer ] (function
          | Ok n ->
            let count = Unsigned.Size_t.to_int n in
            if count = 0 then Ivar.fill ivar `Eof else Ivar.fill ivar (`Ok count)
          | Error e ->
            Error.raise_s
              [%message "Could not read from fd" ~error:(Luv.Error.strerror e)]))
;;
