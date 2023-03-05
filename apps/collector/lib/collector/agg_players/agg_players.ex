defmodule Collector.AggPlayers do
  @enforce_keys [
    :target_dt,
    :server_id,
    :player_id,
    :estimated_starting_date,
    :estimated_tribe,
    :increment
  ]

  defstruct [
    :target_dt,
    :server_id,
    :player_id,
    :estimated_starting_date,
    :estimated_tribe,
    :increment
  ]

  @type t :: %__MODULE__{
          target_dt: DateTime.t(),
          server_id: TTypes.server_id(),
          player_id: TTypes.player_id(),
          estimated_starting_date: Date.t(),
          estimated_tribe: TTypes.tribe_integer(),
          increment: Collector.AggPlayers.Increment.t()
        }

  @spec run(root_folder :: String.t(), server_id :: TTypes.server_id(), target_dt :: DateTime.t()) ::
          :ok | {:error, any()}
  def run(root_folder, server_id, target_dt) do
  end

  @spec process(
          target_dt :: DateTime.t(),
          server_id :: TTypes.server_id(),
          today_snapshot :: [TTypes.enriched_row()],
          yesterday_snapshot :: [TTypes.enriched_row()],
          yesterday_agg_players :: [t()]
        ) :: [t()]
  def process(target_dt, server_id, new_snapshot, prev_snapshot, prev_agg_players) do
    uniq_players = for row <- new_snapshot, uniq: true, do: row.player_id
    find_player = fn player_id -> Enum.find(prev_agg_players, &(&1.player_id == player_id)) end
    for player_id <- uniq_players, do: group_compute_update(target_dt, server_id, player_id, new_snapshot, prev_snapshot, find_player.(player_id))
  end

  defp group_compute_update(target_dt, server_id, player_id, new_snapshot, _prev_snapshot, nil) do
    rows = Enum.filter(new_snapshot, &(&1.player_id == player_id))
    init_struct(target_dt, server_id, player_id, rows) 
  end
  defp group_compute_update(target_dt, _, player_id, new_snapshot, prev_snapshot, prev_agg) do
    {new_player_snapshot, prev_player_snapshot} = group_rows_by_player(player_id, new_snapshot, prev_snapshot)
    new_increment = Collector.AggPlayers.Increment.increment(target_dt, player_id, new_player_snapshot, prev_player_snapshot)

    prev_agg
    |> Map.put(:target_dt, target_dt)
    |> Map.update!(:increment, fn list_of_increments-> [new_increment | list_of_increments] end)
  end



  defp group_rows_by_player(player_id, new_snapshot, prev_snapshot) do
    new_village_ids = MapSet.new(for row <- new_snapshot, row.player_id == player_id, do: row.village_id)
    prev_village_ids = MapSet.new(for row <- prev_snapshot, row.player_id == player_id, do: row.village_id)

    common_villages_ids = MapSet.union(new_village_ids, prev_village_ids) |> MapSet.to_list()

    new_player_rows = for row <- new_snapshot, row.village_id in common_villages_ids, do: row
    prev_player_rows = for row <- prev_snapshot, row.village_id in common_villages_ids, do: row

    {new_player_rows, prev_player_rows}
  end


  @spec process(
          target_dt :: DateTime.t(),
          server_id :: TTypes.server_id(),
          new_snapshot :: [TTypes.enriched_row()]
        ) :: [t()]
  def process(target_dt, server_id, new_snapshot) do
    new_snapshot
    |> Enum.group_by(&(&1.player_id))
    |> Enum.map(fn {player_id, rows} -> init_struct(target_dt, server_id, player_id, rows) end)
  end

  defp init_struct(target_dt, server_id, player_id, rows) do
    %__MODULE__{
      target_dt: target_dt,
      server_id: server_id,
      player_id: player_id,
      estimated_starting_date: DateTime.to_date(target_dt),
      estimated_tribe: get_max_tribe(rows),
      increment: Collector.AggPlayers.Increment.increment(target_dt, rows)
    }
  end

  defp get_max_tribe(rows) do
    Enum.map(rows, &(&1.tribe))
    |> Enum.frequencies()
    |> Enum.max_by(fn {_tribe, freq} -> freq end)
    |> elem(0)
  end








end
