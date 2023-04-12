defmodule Collector.RawSnapshot do
  @behaviour Collector.Feed

  @impl true
  def options(), do: {"raw_snapshot", ".c6bert"}

  @impl true
  def to_format(raw_snapshot),
    do: :erlang.term_to_binary(raw_snapshot, [:compressed, :deterministic])

  @impl true
  def from_format(encoded_raw_snapshot),
    do: :erlang.binary_to_term(encoded_raw_snapshot)

  @impl true
  @spec run(root_folder :: String.t(), server_id :: TTypes.server_id(), target_date :: Date.t()) ::
          :ok | {:error, any()}
  def run(root_folder, server_id, target_date) do
    with(
      {:a, {:ok, raw_snapshot}} <- {:a, :travianmap.get_map(server_id)},
      {:b, :ok} <-
        {:b, Collector.Feed.store(root_folder, server_id, target_date, raw_snapshot, __MODULE__)}
    ) do
      :ok
    else
      {:a, {:error, reason}} -> {:error, {"Unable to fetch server from Travian", reason}}
      {:b, {:error, reason}} -> {:error, {"Unable to store raw_snapshot", reason}}
    end
  end
end
