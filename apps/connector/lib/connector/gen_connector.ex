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
    front_node = Application.fetch_env!(:connector, :front_node)

    case Node.list() do
      [^front_node] ->
        {:noreply, state}

      [] ->
        case Node.connect(front_node) do
          true ->
            Logger.notice(%{msg: "Connected to front node", node: front_node})
            {:noreply, state}

          false ->
            Logger.alert(%{msg: "Can't connect to front node", node: front_node})
            {:noreply, state}

          :ignored ->
            Logger.alert(%{msg: "Can't reach front node", node: front_node})
            {:noreply, state}
        end
    end
  end

  def handle_info(_, state), do: {:noreply, state}
end
