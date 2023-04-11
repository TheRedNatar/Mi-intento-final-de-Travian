defmodule Collector.AggServer.Increment do
  @enforce_keys [
    :target_dt,
    :natar_villages,
    :natar_population,
    :natar_population_variation,
    :total_population,
    :population_variation,
    :total_villages,
    :new_villages,
    :removed_villages,
    :total_players,
    :new_players,
    :removed_players,
    :total_alliances,
    :new_alliances,
    :removed_alliances
  ]

  defstruct [
    :target_dt,
    :natar_villages,
    :natar_population,
    :natar_population_variation,
    :total_population,
    :population_variation,
    :total_villages,
    :new_villages,
    :removed_villages,
    :total_players,
    :new_players,
    :removed_players,
    :total_alliances,
    :new_alliances,
    :removed_alliances
  ]

  @type t :: %__MODULE__{
          target_dt: DateTime.t(),
          natar_villages: non_neg_integer(),
          natar_population: number(),
          natar_population_variation: nil | integer(),
          total_population: number(),
          population_variation: nil | integer(),
          total_villages: non_neg_integer(),
          new_villages: nil | non_neg_integer(),
          removed_villages: nil | non_neg_integer(),
          total_players: non_neg_integer(),
          new_players: nil | non_neg_integer(),
          removed_players: nil | non_neg_integer(),
          total_alliances: non_neg_integer(),
          new_alliances: nil | non_neg_integer(),
          removed_alliances: nil | non_neg_integer()
        }

  @spec increment(target_dt :: DateTime.t(), snapshot :: [Collector.Snapshot.t()]) :: t()
  def increment(target_dt, snapshot) do
    {s_normal, s_natar} = Enum.split_with(snapshot, &(&1.tribe != 5))

    %__MODULE__{
      target_dt: target_dt,
      natar_villages: Enum.count(s_natar),
      natar_population: Enum.map(s_natar, & &1.population) |> Enum.sum(),
      natar_population_variation: nil,
      total_population: Enum.map(s_normal, & &1.population) |> Enum.sum(),
      population_variation: nil,
      total_villages: Enum.count(s_normal),
      new_villages: nil,
      removed_villages: nil,
      total_players: Enum.uniq_by(s_normal, & &1.player_id) |> Enum.count(),
      new_players: nil,
      removed_players: nil,
      total_alliances: Enum.uniq_by(s_normal, & &1.alliance_id) |> Enum.count(),
      new_alliances: nil,
      removed_alliances: nil
    }
  end

  @spec increment(
          target_dt :: DateTime.t(),
          new_snapshot :: [Collector.Snapshot.t()],
          prev_snapshot :: [Collector.Snapshot.t()],
          prev_increment :: t()
        ) :: t()
  def increment(target_dt, new_snapshot, prev_snapshot, prev_increment) do
    {prev_s_normal, _prev_s_natar} = Enum.split_with(prev_snapshot, &(&1.tribe != 5))
    {s_normal, s_natar} = Enum.split_with(new_snapshot, &(&1.tribe != 5))

    prev_village_ids = Enum.map(prev_s_normal, & &1.village_id) |> Enum.uniq()
    new_village_ids = Enum.map(s_normal, & &1.village_id) |> Enum.uniq()

    prev_player_ids = Enum.map(prev_s_normal, & &1.player_id) |> Enum.uniq()
    new_player_ids = Enum.map(s_normal, & &1.player_id) |> Enum.uniq()

    prev_alliance_ids = Enum.map(prev_s_normal, & &1.alliance_id) |> Enum.uniq()
    new_alliance_ids = Enum.map(s_normal, & &1.alliance_id) |> Enum.uniq()

    natar_population = Enum.map(s_natar, & &1.population) |> Enum.sum()
    total_population = Enum.map(s_normal, & &1.population) |> Enum.sum()

    %__MODULE__{
      target_dt: target_dt,
      natar_villages: Enum.count(s_natar),
      natar_population: natar_population,
      natar_population_variation: natar_population - prev_increment.natar_population,
      total_population: total_population,
      population_variation: total_population - prev_increment.total_population,
      total_villages: Enum.count(s_normal),
      new_villages: diff_items(new_village_ids, prev_village_ids) |> Enum.count(),
      removed_villages: diff_items(prev_village_ids, new_village_ids) |> Enum.count(),
      total_players: Enum.count(new_player_ids),
      new_players: diff_items(new_player_ids, prev_player_ids) |> Enum.count(),
      removed_players: diff_items(prev_player_ids, new_player_ids) |> Enum.count(),
      total_alliances: Enum.count(new_alliance_ids),
      new_alliances: diff_items(new_alliance_ids, prev_alliance_ids) |> Enum.count(),
      removed_alliances: diff_items(prev_alliance_ids, new_alliance_ids) |> Enum.count()
    }
  end

  defp diff_items(s1, s2) do
    s1_s = MapSet.new(s1)
    s2_s = MapSet.new(s2)
    MapSet.difference(s1_s, s2_s) |> MapSet.to_list()
  end
end
