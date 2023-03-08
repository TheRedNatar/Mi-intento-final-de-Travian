defmodule Collector.AggPlayers.Increment do
  @enforce_keys [
    :target_dt,
    :total_population,
    :population_increase,
    :population_increase_by_founded,
    :population_increase_by_conquered,
    :population_decrease,
    :population_decrease_by_conquered,
    :population_decrease_by_destroyed,
    :total_villages,
    :n_villages_with_population_increase,
    :n_villages_with_population_decrease,
    :n_villages_with_population_stuck,
    :new_village_founded,
    :new_village_conquered,
    :lost_village_conquered,
    :lost_village_destroyed
  ]

  defstruct [
    :target_dt,
    :total_population,
    :population_increase,
    :population_increase_by_founded,
    :population_increase_by_conquered,
    :population_decrease,
    :population_decrease_by_conquered,
    :population_decrease_by_destroyed,
    :total_villages,
    :n_villages_with_population_increase,
    :n_villages_with_population_decrease,
    :n_villages_with_population_stuck,
    :new_village_founded,
    :new_village_conquered,
    :lost_village_conquered,
    :lost_village_destroyed
  ]

  @type t :: %__MODULE__{
          target_dt: DateTime.t(),
          total_population: non_neg_integer(),
          population_increase: non_neg_integer() | nil,
          population_increase_by_founded: non_neg_integer() | nil,
          population_increase_by_conquered: non_neg_integer() | nil,
          population_decrease: non_neg_integer() | nil,
          population_decrease_by_conquered: non_neg_integer() | nil,
          population_decrease_by_destroyed: non_neg_integer() | nil,
          total_villages: pos_integer(),
          n_villages_with_population_increase: non_neg_integer() | nil,
          n_villages_with_population_decrease: non_neg_integer() | nil,
          n_villages_with_population_stuck: non_neg_integer() | nil,
          new_village_founded: non_neg_integer() | nil,
          new_village_conquered: non_neg_integer() | nil,
          lost_village_conquered: non_neg_integer() | nil,
          lost_village_destroyed: non_neg_integer() | nil
        }

  @spec increment(
          target_dt :: DateTime.t(),
          player_id :: TTypes.player_id(),
          new_player_snapshot :: [TTypes.enriched_row()],
          prev_player_snapshot :: [TTypes.enriched_row()]
        ) :: t()
  def increment(target_dt, player_id, new_player_snapshot, prev_player_snapshot) do
    village_ids_new_founded =
      new_village_founded(player_id, new_player_snapshot, prev_player_snapshot)

    village_ids_new_conquered =
      new_village_conquered(player_id, new_player_snapshot, prev_player_snapshot)

    village_ids_lost_conquered =
      lost_village_conquered(player_id, new_player_snapshot, prev_player_snapshot)

    village_ids_lost_destroyed =
      lost_village_destroyed(player_id, new_player_snapshot, prev_player_snapshot)

    tuples_of_villages_keeped =
      create_tuples(player_id, new_player_snapshot, prev_player_snapshot)

    new_own_villages = for row <- new_player_snapshot, row.player_id == player_id, do: row

    %__MODULE__{
      target_dt: target_dt,
      total_population: sum_pop(new_own_villages),
      population_increase:
        Enum.filter(tuples_of_villages_keeped, fn {_v_id, pop_inc} -> pop_inc > 0 end)
        |> Enum.map(&elem(&1, 1))
        |> Enum.sum(),
      population_increase_by_founded:
        Enum.filter(new_player_snapshot, &(&1.village_id in village_ids_new_founded)) |> sum_pop(),
      population_increase_by_conquered:
        Enum.filter(new_player_snapshot, &(&1.village_id in village_ids_new_conquered))
        |> sum_pop(),
      population_decrease:
        Enum.filter(tuples_of_villages_keeped, fn {_v_id, pop_inc} -> pop_inc < 0 end)
        |> Enum.map(&(elem(&1, 1) * -1))
        |> Enum.sum(),
      population_decrease_by_conquered:
        Enum.filter(new_player_snapshot, &(&1.village_id in village_ids_lost_conquered))
        |> sum_pop(),
      population_decrease_by_destroyed:
        Enum.filter(prev_player_snapshot, &(&1.village_id in village_ids_lost_destroyed))
        |> sum_pop(),
      total_villages: length(new_own_villages),
      n_villages_with_population_increase:
        Enum.filter(tuples_of_villages_keeped, fn {_v_id, pop_inc} -> pop_inc > 0 end)
        |> Enum.count(),
      n_villages_with_population_decrease:
        Enum.filter(tuples_of_villages_keeped, fn {_v_id, pop_inc} -> pop_inc < 0 end)
        |> Enum.count(),
      n_villages_with_population_stuck:
        Enum.filter(tuples_of_villages_keeped, fn {_v_id, pop_inc} -> pop_inc == 0 end)
        |> Enum.count(),
      new_village_founded: length(village_ids_new_founded),
      new_village_conquered: length(village_ids_new_conquered),
      lost_village_conquered: length(village_ids_lost_conquered),
      lost_village_destroyed: length(village_ids_lost_destroyed)
    }
  end

  defp sum_pop(snapshot_rows), do: Enum.map(snapshot_rows, & &1.population) |> Enum.sum()

  defp new_village_conquered(player_id, new_player_snapshot, prev_player_snapshot) do
    prev_village_ids_owned_by_other_player =
      for row <- prev_player_snapshot, row.player_id != player_id, do: row.village_id

    for row <- new_player_snapshot,
        row.player_id == player_id and row.village_id in prev_village_ids_owned_by_other_player,
        do: row.village_id
  end

  defp new_village_founded(player_id, new_player_snapshot, prev_player_snapshot) do
    prev_village_ids = for row <- prev_player_snapshot, do: row.village_id

    for row <- new_player_snapshot,
        row.player_id == player_id and row.village_id not in prev_village_ids,
        do: row.village_id
  end

  defp lost_village_destroyed(player_id, new_player_snapshot, prev_player_snapshot) do
    prev_village_ids =
      for row <- prev_player_snapshot, row.player_id == player_id, do: row.village_id

    non_destroyed = for row <- new_player_snapshot, do: row.village_id
    prev_village_ids -- non_destroyed
  end

  defp lost_village_conquered(player_id, new_player_snapshot, prev_player_snapshot) do
    prev_village_ids =
      for row <- prev_player_snapshot, row.player_id == player_id, do: row.village_id

    for row <- new_player_snapshot,
        row.player_id != player_id and row.village_id in prev_village_ids,
        do: row.village_id
  end

  defp village_ids_keeped(player_id, new_player_snapshot, prev_player_snapshot) do
    prev_village_ids =
      MapSet.new(for row <- prev_player_snapshot, row.player_id == player_id, do: row.village_id)

    new_village_ids =
      MapSet.new(for row <- new_player_snapshot, row.player_id == player_id, do: row.village_id)

    MapSet.intersection(new_village_ids, prev_village_ids) |> MapSet.to_list()
  end

  defp create_tuples(player_id, new_player_snapshot, prev_player_snapshot) do
    village_ids_keeped =
      village_ids_keeped(player_id, new_player_snapshot, prev_player_snapshot) |> Enum.sort()

    new_pops =
      Enum.filter(new_player_snapshot, &(&1.village_id in village_ids_keeped))
      |> Enum.sort_by(& &1.village_id)
      |> Enum.map(& &1.population)

    prev_pops =
      Enum.filter(prev_player_snapshot, &(&1.village_id in village_ids_keeped))
      |> Enum.sort_by(& &1.village_id)
      |> Enum.map(& &1.population)

    Enum.zip([village_ids_keeped, new_pops, prev_pops])
    |> Enum.map(fn {village_id, new_pop, prev_pop} -> {village_id, new_pop - prev_pop} end)
  end

  @spec increment(target_dt :: DateTime.t(), new_player_snapshot :: [TTypes.enriched_row()]) ::
          t()
  def increment(target_dt, new_player_snapshot) do
    %__MODULE__{
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
