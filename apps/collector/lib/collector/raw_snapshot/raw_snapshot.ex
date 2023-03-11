defmodule Collector.RawSnapshot do
  def options(), do: {"raw_snapshot", ".c6bert"}

  defp raw_snapshot_to_format(raw_snapshot),
    do: :erlang.term_to_binary(raw_snapshot, [:compressed, :deterministic])

  defp raw_snapshot_from_format(encoded_raw_snapshot),
    do: :erlang.binary_to_term(encoded_raw_snapshot)

  @spec open(root_folder :: String.t(), server_id :: TTypes.server_id(), target_date :: Date.t()) ::
          {:ok, String.t()} | {:error, any()}
  def open(root_folder, server_id, target_date) do
    case Storage.open(root_folder, server_id, options(), target_date) do
      {:ok, {_, encoded}} -> {:ok, raw_snapshot_from_format(encoded)}
      error -> error
    end
  end

  @spec store(
          root_folder :: String.t(),
          server_id :: TTypes.server_id(),
          raw_snapshot :: String.t(),
          target_date :: Date.t()
        ) :: :ok | {:error, any()}
  def store(root_folder, server_id, raw_snapshot, target_date) do
    encoded = raw_snapshot_to_format(raw_snapshot)
    Storage.store(root_folder, server_id, options(), encoded, target_date)
  end

  @spec run(root_folder :: String.t(), server_id :: TTypes.server_id(), target_date :: Date.t()) ::
          :ok | {:error, any()}
  def run(root_folder, server_id, target_date) do
    with(
      {:a, {:ok, raw_snapshot}} <- {:a, :travianmap.get_map(server_id)},
      {:b, :ok} <- {:b, store(root_folder, server_id, raw_snapshot, target_date)}
    ) do
      :ok
    else
      {:a, {:error, reason}} -> {:error, {"Unable to fetch server from Travian", reason}}
      {:b, {:error, reason}} -> {:error, {"Unable to store raw_snapshot", reason}}
    end
  end
end
