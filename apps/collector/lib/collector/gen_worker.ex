defmodule Collector.GenWorker do
  use GenServer
  require Logger

  @spec start_link(
          root_folder :: String.t(),
          server_id :: TTypes.server_id(),
          target_date :: Date.t(),
          attemps :: pos_integer(),
          min :: pos_integer(),
          max :: pos_integer()
        ) ::
          GenServer.on_start()
  def start_link(root_folder, server_id, target_date, attemps, min, max),
    do: GenServer.start_link(__MODULE__, [root_folder, server_id, target_date, attemps, min, max])

  @impl true
  def init([root_folder, server_id, target_date, attemps, min, max]) do
    {:ok, [root_folder, server_id, target_date, attemps, min, max], {:continue, :init}}
  end

  @impl true
  def handle_continue(:init, state = [root_folder, server_id, target_date, attemps, min, max]) do
    case Collector.DAG.run(root_folder, server_id, target_date, attemps, min, max) do
      :ok ->
        GenServer.cast(Collector.GenCollector, {:ok, server_id, self()})
        {:stop, :normal, state}

      {:error, reason} ->
        GenServer.cast(Collector.GenCollector, {:error, server_id, self()})

        Logger.warning(%{
          msg: "Unable to collect #{server_id}",
          reason: reason,
          target_date: target_date,
          server_id: server_id
        })

        {:stop, :normal, state}
    end
  end
end
