defmodule Collector.AggServer do
  require Logger

  @enforce_keys [
    :target_date,
    :extraction_date,
    :server_id,
    :server_url,
    :server_contraction,
    :server_speed,
    :server_region,
    :estimated_starting_date,
    :villages,
    :total_villages,
    :new_villages,
    :removed_villages,
    :players,
    :total_players,
    :new_players,
    :removed_players,
    :alliances,
    :total_alliances,
    :new_alliances,
    :removed_alliances
  ]

  defstruct [
    :target_date,
    :extraction_date,
    :server_id,
    :server_url,
    :server_contraction,
    :server_speed,
    :server_region,
    :estimated_starting_date,
    :villages,
    :total_villages,
    :new_villages,
    :removed_villages,
    :players,
    :total_players,
    :new_players,
    :removed_players,
    :alliances,
    :total_alliances,
    :new_alliances,
    :removed_alliances
  ]

  @type t :: %__MODULE__{
          target_date: DateTime.t(),
          extraction_date: DateTime.t(),
          server_id: TTypes.server_id(),
	  server_url: String.t(),
          server_contraction: nil | String.t(),
          server_speed: nil | pos_integer(),
          server_region: nil | String.t(),
          estimated_starting_date: Date.t(),
	  villages: [TTypes.village_id()],
	  total_villages: non_neg_integer(),
	  new_villages: nil | non_neg_integer(),
	  removed_villages: nil | neg_integer(),
	  players: [TTypes.player_id()],
	  total_players: non_neg_integer(),
	  new_players: nil | non_neg_integer(),
	  removed_players: nil | neg_integer(),
	  alliances: [TTypes.alliance_id()],
	  total_alliances: non_neg_integer(),
	  new_alliances: nil | non_neg_integer(),
	  removed_alliances: nil | neg_integer()
        }

  @spec run(root_folder :: String.t(), server_id :: TTypes.server_id(), target_date :: Date.t()) :: :ok | {:error, any()}
  def run(root_folder, server_id, target_date) when is_struct(target_date, Date) do

    Logger.info(%{msg: "Running AggServer", server_id: server_id, target_date: target_date})
    with(
      {:a, {:ok, {_, encoded_snapshot}}} <- {:a, Storage.open(root_folder, server_id, Collector.snapshot_options(), target_date)},
      snapshot = Collector.snapshot_from_format(encoded_snapshot),
      Logger.info(%{msg: "Snapshot opened", server_id: server_id, target_date: target_date}),
      {:b, {:ok, agg_server_yesterday}} <- {:b, open_agg_server_yesterday(root_folder, server_id, target_date)},
      Logger.info(%{msg: "Yesterday's AggServer opened", server_id: server_id, target_date: target_date}),
      agg_server = process(server_id, target_date, snapshot, agg_server_yesterday),
      Logger.info(%{msg: "AggServer processed", server_id: server_id, target_date: target_date}),
      encoded_agg_server = Collector.agg_server_to_format(agg_server),
      {:c, :ok} <- {:c, Storage.store(root_folder, server_id, Collector.agg_server_options(), encoded_agg_server, target_date)},
      Logger.info(%{msg: "AggServer stored", server_id: server_id, target_date: target_date}))
    do 
      Logger.info(%{msg: "AggServer success", server_id: server_id, target_date: target_date})
      :ok
    else
      {:a, {:error, reason}} ->
	Logger.info(%{msg: "Unable to open Snapshot", reason: reason, server_id: server_id, target_date: target_date})
      {:error, reason}
      {:b, {:error, reason}} ->
	Logger.info(%{msg: "Unable to open yesterday's AggServer", reason: reason, server_id: server_id, target_date: target_date})
      {:error, reason}
      {:c, {:error, reason}} ->
	Logger.info(%{msg: "Unable to store AggServer", reason: reason, server_id: server_id, target_date: target_date})
      {:error, reason}
    end
  end

  defp open_agg_server_yesterday(root_folder, server_id, target_date) do
    yesterday = Date.add(target_date, -1)
    case Storage.exist?(root_folder, server_id, Collector.agg_server_options, yesterday) do
      false -> {:ok, nil}
      true -> case Storage.open(root_folder, server_id, Collector.agg_server_options, yesterday) do
		{:ok, {_, encoded_agg_server}} -> {:ok, Collector.agg_server_from_format(encoded_agg_server)}
		{:error, reason} -> {:error, reason}
	      end
    end
  end

  @spec process(server_id :: TTypes.server_id(), target_date:: Date.t(), snapshot :: [Collector.SnapshotRow.t()], agg_server_yesterday :: nil | t()) :: t()
  def process(server_id, target_date, snapshot, agg_server_yesterday)
  def process(server_id, target_date, snapshot, nil) do

    {contraction, speed, region} = server_metadata(server_id)
    village_ids = Enum.map(snapshot, fn x -> x.village_id end)
    player_ids = Enum.map(snapshot, fn x -> x.player_id end) |> Enum.uniq()
    alliance_ids = Enum.map(snapshot, fn x -> x.alliance_id end) |> Enum.uniq()
  %__MODULE__{
          target_date: target_date,
          extraction_date: DateTime.utc_now(),
          server_id: server_id,
	  server_url: server_id,
          server_contraction: contraction,
          server_speed: speed,
          server_region: region,
          estimated_starting_date: target_date,
	  villages: village_ids,
	  total_villages: length(snapshot),
	  new_villages: nil,
	  removed_villages: nil,
	  players: player_ids,
	  total_players: length(player_ids),
	  new_players: nil,
	  removed_players: nil,
	  alliances: alliance_ids,
	  total_alliances: length(alliance_ids),
	  new_alliances: nil,
	  removed_alliances: nil
        }
  end

  def process(server_id, target_date, snapshot, agg_server_yesterday) do

    village_ids = Enum.map(snapshot, fn x -> x.village_id end)
    player_ids = Enum.map(snapshot, fn x -> x.player_id end) |> Enum.uniq()
    alliance_ids = Enum.map(snapshot, fn x -> x.alliance_id end) |> Enum.uniq()

  %__MODULE__{
          target_date: target_date,
          extraction_date: DateTime.utc_now(),
          server_id: server_id,
	  server_url: agg_server_yesterday.server_id,
          server_contraction: agg_server_yesterday.server_contraction,
          server_speed: agg_server_yesterday.server_speed,
          server_region: agg_server_yesterday.server_region,
          estimated_starting_date: agg_server_yesterday.estimated_starting_date,
	  villages: village_ids,
	  total_villages: length(village_ids),
	  new_villages: village_ids -- agg_server_yesterday.villages,
	  removed_villages: agg_server_yesterday.villages -- village_ids,
	  players: player_ids,
	  total_players: length(player_ids),
	  new_players: player_ids -- agg_server_yesterday.players,
	  removed_players: agg_server_yesterday.players -- player_ids,
	  alliances: alliance_ids,
	  total_alliances: length(alliance_ids),
	  new_alliances: alliance_ids -- agg_server_yesterday.alliances,
	  removed_alliances: agg_server_yesterday.alliances -- alliance_ids
        }
  end




  defp server_metadata(server_id) do
    case TTypes.get_metadata_from_server_id(server_id) do
      {:error, :not_splitable} -> {nil, nil, nil}
      {:ok, {contraction, speed_string, region}} ->
	{contraction, TTypes.speed_string_to_int(speed_string), region}
    end
  end


end
