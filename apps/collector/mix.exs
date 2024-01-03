defmodule Collector.MixProject do
  use Mix.Project

  def project do
    [
      app: :collector,
      version: "0.1.0",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.11",
      start_permanent: Mix.env() == :prod,
      dialyzer: [plt_add_apps: [:mnesia]],
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger, :ssl, :inets, :mnesia],
      mod: {Collector.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:dialyxir, "~> 1.0", only: [:dev], runtime: false},
      {:benchee, "~> 1.0", only: :dev},
      {:retryable_ex, "~> 2.0"},
      {:travianmap, "~> 1.0.0"},
      {:flow, "~> 1.0"},
      {:temp, "~> 0.4.7"},
      {:t_types, in_umbrella: true},
      {:storage, in_umbrella: true}
    ]
  end
end
