type t [@@deriving sexp_of]

include module type of struct
  include Async_kernel.Async_kernel_scheduler
end

val run : unit -> Core_kernel.never_returns
