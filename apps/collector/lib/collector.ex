defmodule Collector do
  @moduledoc """
  Documentation for `Collector`.
  """

  @doc """
  Launch the collection process
  """
  @spec collect() :: :ok
  def collect(), do: Collector.GenCollector.collect()

  @doc """
  Subscribe the process to the `Collector`. When a server is collected, the subscriber
  will receive {:collected, type, server_id}. It also monitors the `Collector`.
  """
  @spec subscribe() :: reference()
  def subscribe(), do: Collector.GenCollector.subscribe()

  def set_up_mnesia(nodes, master_node) do
    :rpc.multicall(nodes, :application, :stop, [:mnesia])

    with(
      :ok <- :mnesia.delete_schema(nodes),
      :ok <- :mnesia.create_schema(nodes),
      :rpc.multicall(nodes, :application, :start, [:mnesia]),
      {:atomic, _res} <- Collector.Feed.create_table(nodes, Collector.SServer),
      {:atomic, _res} <- Collector.Feed.create_table(nodes, Collector.SMedusaPred),
      :ok <- :mnesia.set_master_nodes([master_node])
    ) do
      :ok
    else
      reason -> {:error, reason}
    end
  end

  def clear_tables() do
    :mnesia.clear_table(:s_server)
    :mnesia.clear_table(:s_medusa_pred)
  end
end
