defmodule Collector.IncrementalServer.Entities do
  @enforce_keys [
    :target_date,
    :total_population,
    :total_population_no_natar,
    :total_population_increase,
    :total_population_increase_no_natar,
    :total_population_decrease,
    :total_population_decrease_no_natar,
    :total_villages,
    :total_villages_no_natar,
    :new_villages,
    :new_villages_no_natar,
    :removed_villages,
    :removed_villages_no_natar,
    :conquered_villages,
    :conquered_villages_no_natar,
    :total_players,
    :new_players,
    :removed_players,
    :total_alliances,
    :new_alliances,
    :removed_alliances
  ]

  defstruct [
    :target_date,
    :total_population,
    :total_population_no_natar,
    :total_population_increase,
    :total_population_increase_no_natar,
    :total_population_decrease,
    :total_population_decrease_no_natar,
    :total_villages,
    :total_villages_no_natar,
    :new_villages,
    :new_villages_no_natar,
    :removed_villages,
    :removed_villages_no_natar,
    :conquered_villages,
    :conquered_villages_no_natar,
    :total_players,
    :new_players,
    :removed_players,
    :total_alliances,
    :new_alliances,
    :removed_alliances
  ]


  @type t :: %__MODULE__{
    target_date: DateTime.t(),
    total_population: non_neg_integer(),
    total_population_no_natar: non_neg_integer(),
    total_population_increase: nil | non_neg_integer(),
    total_population_increase_no_natar: nil | non_neg_integer(),
    total_population_decrease: nil | non_neg_integer(),
    total_population_decrease_no_natar: nil | non_neg_integer(),
    total_villages: pos_integer(),
    total_villages_no_natar: pos_integer(),
    new_villages: nil | non_neg_integer(),
    new_villages_no_natar: nil | non_neg_integer(),
    removed_villages: nil | non_neg_integer(),
    removed_villages_no_natar: nil | non_neg_integer(),
    conquered_villages: nil | non_neg_integer(),
    conquered_villages_no_natar: nil | non_neg_integer(),
    total_players: pos_integer()
    new_players: nil | non_neg_integer(),
    removed_players: nil | non_neg_integer(),
    total_alliances: non_neg_integer(),
    new_alliances: nil | non_neg_integer(),
    removed_alliances: nil | non_neg_integer()
    }
end
