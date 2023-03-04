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
          today_snapshot :: [TTypes.enriched_row()],
          yesterday_snapshot :: [TTypes.enriched_row()] | nil,
          yesterday_agg_players :: [t()] | nil
        ) :: [t()]
  def process(target_dt, today_snapshot, yesterday_snapshot, yesterday_agg_players)
  def process(target_dt, today_snapshot, nil, nil), do: :ok
  def process(target_dt, today_snapshot, yesterday_snapshot, yesterday_agg_players), do: :ok



  @spec increment(target_dt :: DateTime.t(), player_id :: TTypes.player_id(), new_player_snapshot :: [TTypes.enriched_row()], prev_player_snapshot :: [TTypes.enriched_row()]) :: Collector.AggPlayers.Increment.t()
  def increment(target_dt, player_id, new_player_snapshot, prev_player_snapshot) do

    village_ids_new_founded =  new_village_founded(player_id, new_player_snapshot, prev_player_snapshot)
    village_ids_new_conquered =  new_village_conquered(player_id, new_player_snapshot, prev_player_snapshot)
    village_ids_lost_conquered = lost_village_conquered(player_id, new_player_snapshot, prev_player_snapshot)
    village_ids_lost_destroyed = lost_village_destroyed(player_id, new_player_snapshot, prev_player_snapshot)

    tuples_of_villages_keeped = create_tuples(player_id, new_player_snapshot, prev_player_snapshot)

    %Collector.AggPlayers.Increment{
      target_dt: target_dt,
      total_population: sum_pop(new_player_snapshot),
      population_increase: Enum.filter(tuples_of_villages_keeped, fn {v_id, pop_inc} -> pop_inc > 0 end) |> Enum.map(&(elem(&1, 1))) |> Enum.sum(),
      population_increase_by_founded: Enum.filter(new_player_snapshot, &(&1.village_id) in village_ids_new_founded) |> sum_pop(),
      population_increase_by_conquered: Enum.filter(new_player_snapshot, &(&1.village_id) in village_ids_new_conquered) |> sum_pop(),
      population_decrease: Enum.filter(tuples_of_villages_keeped, fn {v_id, pop_inc} -> pop_inc < 0 end) |> Enum.map(&(elem(&1, 1) * -1)) |> Enum.sum(),
      population_decrease_by_conquered: Enum.filter(new_player_snapshot, &(&1.village_id) in village_ids_lost_conquered) |> sum_pop(),
      population_decrease_by_destroyed: Enum.filter(prev_player_snapshot, &(&1.village_id) in village_ids_lost_destroyed) |> sum_pop(),
      total_villages: length(new_player_snapshot),
      n_villages_with_population_increase: Enum.filter(tuples_of_villages_keeped, fn {v_id, pop_inc} -> pop_inc > 0 end) |> Enum.count(),
      n_villages_with_population_decrease: Enum.filter(tuples_of_villages_keeped, fn {v_id, pop_inc} -> pop_inc < 0 end) |> Enum.count(),
      n_villages_with_population_stuck: Enum.filter(tuples_of_villages_keeped, fn {v_id, pop_inc} -> pop_inc == 0 end) |> Enum.count(),
      new_village_founded: length(village_ids_new_founded),
      new_village_conquered: length(village_ids_new_conquered),
      lost_village_conquered: length(village_ids_lost_conquered),
      lost_village_destroyed: length(village_ids_lost_destroyed)
    }
  end

  defp sum_pop(snapshot_rows), do: Enum.map(snapshot_rows, &(&1.population)) |> Enum.sum()

  defp new_village_conquered(player_id, new_player_snapshot, prev_player_snapshot) do
    prev_village_ids_owned_by_other_player = for row <- prev_player_snapshot, row.player_id != player_id, do: row.village_id
    for row <- new_player_snapshot, row.player_id == player_id and row.village_id in prev_village_ids_owned_by_other_player, do: row.village_id
  end

  defp new_village_founded(player_id, new_player_snapshot, prev_player_snapshot) do
    prev_village_ids = for row <- prev_player_snapshot, do: row.village_id
    for row <- new_player_snapshot, row.player_id == player_id and row.village_id not in prev_village_ids, do: row.village_id
  end

  defp lost_village_destroyed(player_id, new_player_snapshot, prev_player_snapshot) do
    prev_village_ids = for row <- prev_player_snapshot, row.player_id == player_id, do: row.village_id
    non_destroyed = for row <- new_player_snapshot, do: row.village_id
    prev_village_ids -- non_destroyed
  end

  defp lost_village_conquered(player_id, new_player_snapshot, prev_player_snapshot) do
    prev_village_ids = for row <- prev_player_snapshot, row.player_id == player_id, do: row.village_id
    for row <- new_player_snapshot, row.player_id != player_id and row.village_id in prev_village_ids, do: row.village_id
  end




  defp village_ids_keeped(player_id, new_player_snapshot, prev_player_snapshot) do
    prev_village_ids = MapSet.new(for row <- prev_player_snapshot, row.player_id == player_id, do: row.village_id)
    new_village_ids = MapSet.new(for row <- new_player_snapshot, row.player_id == player_id, do: row.village_id)
    MapSet.intersection(new_village_ids, prev_village_ids) |> MapSet.to_list()
  end

  defp create_tuples(player_id, new_player_snapshot, prev_player_snapshot) do
    village_ids_keeped = village_ids_keeped(player_id, new_player_snapshot, prev_player_snapshot) |> Enum.sort()
    new_pops = Enum.filter(new_player_snapshot, &(&1.village_id in village_ids_keeped)) |> Enum.sort_by(&(&1.village_id)) |> Enum.map(&(&1.population))
    prev_pops = Enum.filter(prev_player_snapshot, &(&1.village_id in village_ids_keeped)) |> Enum.sort_by(&(&1.village_id)) |> Enum.map(&(&1.population))

    Enum.zip([village_ids_keeped, new_pops, prev_pops]) |> Enum.map(fn {village_id, new_pop, prev_pop} -> {village_id, new_pop - prev_pop} end)

  end



  @spec increment(target_dt :: DateTime.t(), new_player_snapshot :: [TTypes.enriched_row()]) :: Collector.AggPlayers.Increment.t()
  def increment(target_dt, new_player_snapshot) do
    %Collector.AggPlayers.Increment{
      target_dt: target_dt,
      total_population: sum_pop(new_player_snapshot),
      population_increase: nil,
      population_increase_by_founded: nil,
      population_increase_by_conquered: nil,
      population_decrease: nil,
      population_decrease_by_conquered: nil,
      population_decrease_by_destroyed: nil,
      total_villages: length(new_player_snapshot),
      n_villages_with_population_increase: nil,
      n_villages_with_population_decrease: nil,
      n_villages_with_population_stuck: nil,
      new_village_founded: nil,
      new_village_conquered: nil,
      lost_village_conquered: nil,
      lost_village_destroyed: nil
    }
  end
end
