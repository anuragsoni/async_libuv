(lang dune 2.9)

(name async_uv)

(generate_opam_files true)

(package
 (name async_uv)
 (depends
  (ocaml
   (>= 4.11.0))
  core_kernel
  async_kernel
  ppx_jane
  luv))
