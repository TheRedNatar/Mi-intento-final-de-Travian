defmodule Collector.GenArchive do
  use GenServer
  require Logger

  @spec start_archiving(root_folder :: String.t(), target_date :: Date.t()) :: :ok
  def start_archiving(root_folder, target_date) do
    GenServer.cast(__MODULE__, {:archive, root_folder, target_date})
  end

  @spec start_link() :: GenServer.on_start()
  def start_link(), do: GenServer.start_link(__MODULE__, [], name: __MODULE__)

  @impl true
  def init([]) do
    {:ok, []}
  end

  @impl true
  def handle_call(_msg, _from, state), do: {:noreply, state}

  @impl true
  def handle_cast({:archive, root_folder, target_date}, state) do
    Logger.info(%{msg: "Starting archive process"})

    f = fn s ->
      Storage.Archive.candidate_for_closing?(root_folder, s, target_date) == {:ok, true}
    end

    candidates = Storage.list_servers(root_folder) |> Enum.filter(f)
    Enum.each(candidates, fn s -> move(root_folder, s) end)

    Logger.info(%{msg: "Archive process finished"})
    {:noreply, state}
  end

  def handle_cast(_msg, state), do: {:noreply, state}

  @impl true
  def handle_info(_msg, state), do: {:noreply, state}

  defp move(root_folder, server_id) do
    case Storage.Archive.move_to_archive(root_folder, server_id) do
      {:ok, new_server_id} ->
        Logger.info(%{msg: "#{server_id} archived -> #{new_server_id}", server_id: server_id})
        :ok

      {:error, reason} ->
        Logger.error(%{
          msg: "#{server_id} error while archiving",
          server_id: server_id,
          reason: reason
        })

        :ok
    end
  end
end
