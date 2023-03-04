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



end
