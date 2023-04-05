defmodule Collector.Supervisor.Worker do
  @moduledoc false
  use DynamicSupervisor
  require Logger

  @spec start_link(any()) :: Supervisor.on_start()
  def start_link(_init_arg) do
    DynamicSupervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def init(_init_arg) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  @spec start_child(
          root_folder :: String.t(),
          server_id :: TTypes.server_id(),
          target_date :: Date.t()
        ) ::
          {:ok, {pid(), reference(), TTypes.server_id()}} | {:error, any()}
  def start_child(root_folder, server_id, target_date) do
    attemps = Application.fetch_env!(:collector, :attemps)
    min = Application.fetch_env!(:collector, :min)
    max = Application.fetch_env!(:collector, :max)

    child_spec = %{
      id: "Collector.GenWorker",
      start:
        {Collector.GenWorker, :start_link,
         [root_folder, server_id, target_date, attemps, min, max]},
      restart: :transient,
      shutdown: :brutal_kill
    }

    case DynamicSupervisor.start_child(__MODULE__, child_spec) do
      {:ok, pid} ->
        ref = Process.monitor(pid)
        {:ok, {pid, ref, server_id}}

      {:error, reason} ->
        Logger.error(%{
          msg: "Unable to launch GenWorker",
          reason: reason,
          server_id: server_id
        })

        {:error, reason}
    end
  end
end
