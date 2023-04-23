defmodule Collector.MedusaTrainTest do
  use ExUnit.Case

  test "A player is considered inactive for last day if the sum of population_increase* of agg_player's increment is 0" do
    inc = %Collector.AggPlayers.Increment{
      target_dt: ~U[2023-04-15 00:00:00.000Z],
      total_population: 1047,
      population_increase: 0,
      population_increase_by_founded: 0,
      population_increase_by_conquered: 0,
      population_decrease: 100,
      population_decrease_by_conquered: 0,
      population_decrease_by_destroyed: 0,
      total_villages: 3,
      n_villages_with_population_increase: 0,
      n_villages_with_population_decrease: 1,
      n_villages_with_population_stuck: 2,
      new_village_founded: 0,
      new_village_conquered: 0,
      lost_village_conquered: 0,
      lost_village_destroyed: 0
    }

    assert(Collector.MedusaTrain.is_inactive?(inc) == true)
    assert(Collector.MedusaTrain.is_inactive?(%{inc | population_increase: 10}) == false)

    assert(
      Collector.MedusaTrain.is_inactive?(%{inc | population_increase_by_founded: 1}) == false
    )

    assert(
      Collector.MedusaTrain.is_inactive?(%{inc | population_increase_by_conquered: 500}) == false
    )
  end

  test "If the it is the initial increment, the output of is_inactive? must be nil" do
    inc = %Collector.AggPlayers.Increment{
      target_dt: ~U[2023-04-15 00:00:00.000Z],
      total_population: 1047,
      population_increase: nil,
      population_increase_by_founded: nil,
      population_increase_by_conquered: nil,
      population_decrease: nil,
      population_decrease_by_conquered: nil,
      population_decrease_by_destroyed: nil,
      total_villages: 3,
      n_villages_with_population_increase: nil,
      n_villages_with_population_decrease: nil,
      n_villages_with_population_stuck: nil,
      new_village_founded: nil,
      new_village_conquered: nil,
      lost_village_conquered: nil,
      lost_village_destroyed: nil
    }

    assert(Collector.MedusaTrain.is_inactive?(inc) == nil)
  end
end
