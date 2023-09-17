defmodule Collector do
  @moduledoc """
  Documentation for `Collector`.
  """

  @doc """
  Launch the collection process
  """
  @spec collect() :: :ok
  def collect(), do: Collector.GenCollector.collect()
end
