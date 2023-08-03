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
end
