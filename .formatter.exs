# Used by "mix format"
[
  import_deps: [:phoenix],
  plugins: [Phoenix.LiveView.HTMLFormatter],
  inputs: ["mix.exs", "config/*.exs", "*.{heex,ex,exs}"],
  subdirectories: ["apps/*"]
]
