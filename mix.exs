defmodule MyTravian.MixProject do
  use Mix.Project

  def project do
    [
      name: "MyTravian project",
      apps_path: "apps",
      version: version(),
      start_permanent: Mix.env() == :prod,
      aliases: aliases(),
      deps: deps(),
      dialyzer: [plt_add_apps: [:mnesia]],
      releases: releases()
    ]
  end

  defp aliases do
    [
      ensure: [
        "format --check-formatted",
        "dialyzer",
        "credo"
      ]
    ]
  end

  defp deps do
    [
      {:dialyxir, "~> 1.0", only: [:dev], runtime: false},
      {:gradient, github: "esl/gradient", only: [:dev], runtime: false},
      {:credo, "~> 1.5", only: [:dev, :test], runtime: false},
      {:benchee, "~> 1.0", only: :dev},
      {:flow, "~> 1.0"},
      {:temp, "~> 0.4.7"},
      {:ex_doc, "~> 0.24", only: :dev, runtime: false}
    ]
  end

  defp releases do
    [
      imperatoris: release_imperatoris(),
      legati: release_legati(),
      monolith: release_monolith()
    ]
  end

  defp release_imperatoris do
    [
      applications: [
        kernel: :permanent,
        stdlib: :permanent,
        sasl: :permanent,
        elixir: :permanent,
        mnesia: :permanent,
        collector: :permanent
      ],
      include_executables_for: [:unix],
      steps: [:assemble, :tar]
    ]
  end

  defp release_legati do
    [
      applications: [
        kernel: :permanent,
        stdlib: :permanent,
        sasl: :permanent,
        elixir: :permanent,
        mnesia: :permanent,
        connector: :permanent,
        front: :permanent
      ],
      include_executables_for: [:unix],
      steps: [:assemble, :tar]
    ]
  end

  defp release_monolith do
    [
      applications: [
        kernel: :permanent,
        stdlib: :permanent,
        sasl: :permanent,
        elixir: :permanent,
        mnesia: :permanent,
        collector: :permanent,
        front: :permanent
      ],
      include_executables_for: [:unix],
      steps: [:assemble, :tar]
    ]
  end

  defp version() do
    {version, 0} = System.cmd("git", ~w[describe --dirty --tags --always --first-parent])

    version
    |> String.split("-")
    |> List.first()
    |> String.replace_prefix("v", "")
    |> String.trim()
  end
end
