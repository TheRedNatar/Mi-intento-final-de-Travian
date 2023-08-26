defmodule Connector.GenConnector do
  use GenServer
  require Logger

  @spec start_link() :: GenServer.on_start()
  def start_link(), do: GenServer.start_link(__MODULE__, [])

  def init([]) do
    {:ok, tref} = :timer.send_interval(2000, :check_connexion)
    {:ok, [tref]}
  end

  def handle_call(_, _, state), do: {:noreply, state}

  def handle_cast(_, state), do: {:noreply, state}

  def handle_info(:check_connexion, state) do
    node_to_ping = Application.fetch_env!(:connector, :node_to_ping)

    case Node.list() do
      [^node_to_ping] ->
        {:noreply, state}

      [] ->
        case Node.connect(node_to_ping) do
          true ->
            Logger.notice(%{msg: "Connected to node", node: node_to_ping})
            {:noreply, state}

          false ->
            Logger.alert(%{msg: "Can't connect to node", node: node_to_ping})
            {:noreply, state}

          :ignored ->
            Logger.alert(%{msg: "Can't reach node", node: node_to_ping})
            {:noreply, state}
        end
    end
  end

  def handle_info(_, state), do: {:noreply, state}
end
