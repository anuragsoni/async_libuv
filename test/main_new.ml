open! Core_kernel
open! Async_kernel
open! Async_libuv

module Codec = struct
  type t =
    { writer : Writer.t
    ; reader : Reader.t
    }

  let create fd =
    let reader = Reader.create fd in
    let writer = Writer.create fd in
    { writer; reader }
  ;;

  let buf' = Bigstring.of_string "+PONG\r\n"

  let run_loop t =
    Reader.read_one_chunk_at_a_time t.reader ~on_chunk:(fun buf ~pos ~len ->
        for i = pos to len - 1 do
          if Char.(Bigstring.get buf i = '\n')
          then Writer.schedule_bigstring t.writer buf'
        done;
        Writer.flush t.writer;
        Reader.Read_chunk_result.Continue)
    >>| function
    | Error `Eof -> ()
    | Error `Closed -> raise_s [%message "Attempting to read from a closed fd"]
    | Ok _ -> ()
  ;;
end

let run () =
  let addr = Luv.Sockaddr.ipv4 "127.0.0.1" 8888 |> Caml.Result.get_ok in
  let _server =
    Tcp.Server.create
      ~backlog:11_000
      ~handle_client:(fun stream ->
        let conn = Codec.create stream in
        Codec.run_loop conn)
      ~on_handler_error:ignore
      addr
  in
  ()
;;

let () =
  run ();
  never_returns (Scheduler.run ())
