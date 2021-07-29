(lang dune 2.9)

(name async_libuv)

(generate_opam_files true)

(package
 (name async_libuv)
 (depends
  (ocaml
   (>= 4.11.0))
  core_kernel
  async_kernel
  ppx_jane
  luv))
