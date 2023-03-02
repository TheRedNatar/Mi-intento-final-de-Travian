defmodule Collector.AggPlayers.Increment do
  @enforce_keys [
    :target_date,
    :total_population,
    :population_increase,
    :population_decrease,
    :total_villages,
    :n_villages_with_population_increase,
    :n_villages_with_population_decrease,
    :n_villages_with_population_stuck,
    :new_village_founded,
    :new_village_conquered,
    :lost_village_conquered,
    :lost_village_destroyed,
    :total_village_founded,
    :total_village_conquered,
    :total_lost_village_conquered,
    :total_lost_village_destroyed
  ]

  defstruct [
    :target_date,
    :total_population,
    :population_increase,
    :population_decrease,
    :total_villages,
    :n_villages_with_population_increase,
    :n_villages_with_population_decrease,
    :n_villages_with_population_stuck,
    :new_village_founded,
    :new_village_conquered,
    :lost_village_conquered,
    :lost_village_destroyed,
    :total_village_founded,
    :total_village_conquered,
    :total_lost_village_conquered,
    :total_lost_village_destroyed
  ]

  @type t :: %__MODULE__{
          target_date: Date.t(),
          total_population: non_neg_integer(),
          population_increase: non_neg_integer() | nil,
          population_decrease: non_neg_integer() | nil,
          total_villages: pos_integer(),
          n_villages_with_population_increase: non_neg_integer() | nil,
          n_villages_with_population_decrease: non_neg_integer() | nil,
          n_villages_with_population_stuck: non_neg_integer() | nil,
          new_village_founded: non_neg_integer() | nil,
          new_village_conquered: non_neg_integer() | nil,
          lost_village_conquered: non_neg_integer() | nil,
          lost_village_destroyed: non_neg_integer() | nil,
          total_village_founded: non_neg_integer(),
          total_village_conquered: non_neg_integer(),
          total_lost_village_conquered: non_neg_integer(),
          total_lost_village_destroyed: non_neg_integer()
        }
end
