defmodule Collector.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  def start(_type, _args) do
    gen_collector = %{
      :id => "gen_collector",
      :start => {Collector.GenCollector, :start_link, []},
      :restart => :permanent,
      :shutdown => 5_000,
      :type => :worker
    }

    gen_archive = %{
      :id => "gen_archive",
      :start => {Collector.GenArchive, :start_link, []},
      :restart => :permanent,
      :shutdown => 5_000,
      :type => :worker
    }

    medusa_model_args = [
      Application.fetch_env!(:collector, :model_dir),
      Application.fetch_env!(:collector, :py_version),
      Application.fetch_env!(:collector, :py_env),
      Application.fetch_env!(:collector, :server),
      Application.fetch_env!(:collector, :model)
    ]

    gen_medusa_port = %{
      :id => "gen_medusa_port",
      :start => {Collector.MedusaPredOutput.GenPort, :start_link, medusa_model_args},
      :restart => :permanent,
      :shutdown => 5_000,
      :type => :worker
    }

    children = [
      Collector.Supervisor.Worker,
      gen_collector,
      gen_archive,
      gen_medusa_port
    ]

    opts = [strategy: :rest_for_one, name: Collector.Supervisor, max_restarts: 8, max_seconds: 30]
    Supervisor.start_link(children, opts)
  end
end
