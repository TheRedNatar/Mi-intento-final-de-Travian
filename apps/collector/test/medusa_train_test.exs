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

  # test "If the it is the initial increment, the output of is_inactive? must be nil" do
  #   inc = %Collector.AggPlayers.Increment{
  #     target_dt: ~U[2023-04-15 00:00:00.000Z],
  #     total_population: 1047,
  #     population_increase: nil,
  #     population_increase_by_founded: nil,
  #     population_increase_by_conquered: nil,
  #     population_decrease: nil,
  #     population_decrease_by_conquered: nil,
  #     population_decrease_by_destroyed: nil,
  #     total_villages: 3,
  #     n_villages_with_population_increase: nil,
  #     n_villages_with_population_decrease: nil,
  #     n_villages_with_population_stuck: nil,
  #     new_village_founded: nil,
  #     new_village_conquered: nil,
  #     lost_village_conquered: nil,
  #     lost_village_destroyed: nil
  #   }

  #   assert(Collector.MedusaTrain.is_inactive?(inc) == nil)
  # end

  @tag :tmp_dir
  test "MedusaTrain.run returns unable to open file if there is no AggPlayers of target_date", %{
    tmp_dir: root_folder
  } do
    target_date = Date.utc_today()
    server_id = "server1"

    # :ok = Collector.Feed.store(root_folder, server_id, target_date, content, Collector.AggPlayers)
    {:error, {msg, _reason}} = Collector.MedusaTrain.run(root_folder, server_id, target_date)
    assert(msg == "Unable to open agg_players")
  end

  @tag :tmp_dir
  test "MedusaTrain.run writes nothing if there is not MedusaPredInput of target_date -1", %{
    tmp_dir: root_folder
  } do
    target_date = Date.utc_today()
    server_id = "server1"
    content = "blabla"
    :ok = Collector.Feed.store(root_folder, server_id, target_date, content, Collector.AggPlayers)
    :ok = Collector.MedusaTrain.run(root_folder, server_id, target_date)

    assert(
      false ==
        Storage.exist?(root_folder, server_id, Collector.MedusaTrain.options(), target_date)
    )
  end

  test "MedusaTrain.process labels the sample of target_date -1 according to the behaviour of the current increment" do
    target_dt = ~U[2023-04-29 00:00:00.000Z]

    active_medusa_pred_input = %Collector.MedusaPredInput{
      target_dt: ~U[2023-04-29 00:00:00.000Z],
      server_id: "https://ts1.x1.international.travian.com",
      player_id: "https://ts1.x1.international.travian.com--P--6319",
      has_alliance?: 1,
      server_days_from_start: 22,
      has_speed?: 1,
      speed: 1,
      player_days_from_start: 3,
      estimated_tribe: 3,
      t_has_increase?: 1,
      t_total_population: 258,
      t_population_increase: 5,
      t_population_increase_by_founded: 0,
      t_population_increase_by_conquered: 0,
      t_population_decrease: 0,
      t_population_decrease_by_conquered: 0,
      t_population_decrease_by_destroyed: 0,
      t_total_villages: 1,
      t_n_villages_with_population_increase: 1,
      t_n_villages_with_population_decrease: 0,
      t_n_villages_with_population_stuck: 0,
      t_new_village_founded: 0,
      t_new_village_conquered: 0,
      t_lost_village_conquered: 0,
      t_lost_village_destroyed: 0,
      t_1_has_data?: 1,
      t_1_has_increase?: 1,
      t_1_time_difference_in_days: 1.0,
      t_1_total_population: 253,
      t_1_population_increase: 50,
      t_1_population_increase_by_founded: 0,
      t_1_population_increase_by_conquered: 0,
      t_1_population_decrease: 0,
      t_1_population_decrease_by_conquered: 0,
      t_1_population_decrease_by_destroyed: 0,
      t_1_total_villages: 1,
      t_1_n_villages_with_population_increase: 1,
      t_1_n_villages_with_population_decrease: 0,
      t_1_n_villages_with_population_stuck: 0,
      t_1_new_village_founded: 0,
      t_1_new_village_conquered: 0,
      t_1_lost_village_conquered: 0,
      t_1_lost_village_destroyed: 0,
      t_2_has_data?: 1,
      t_2_has_increase?: 1,
      t_2_time_difference_in_days: 2.0,
      t_2_total_population: 203,
      t_2_population_increase: 90,
      t_2_population_increase_by_founded: 0,
      t_2_population_increase_by_conquered: 0,
      t_2_population_decrease: 0,
      t_2_population_decrease_by_conquered: 0,
      t_2_population_decrease_by_destroyed: 0,
      t_2_total_villages: 1,
      t_2_n_villages_with_population_increase: 1,
      t_2_n_villages_with_population_decrease: 0,
      t_2_n_villages_with_population_stuck: 0,
      t_2_new_village_founded: 0,
      t_2_new_village_conquered: 0,
      t_2_lost_village_conquered: 0,
      t_2_lost_village_destroyed: 0,
      t_3_has_data?: 1,
      t_3_has_increase?: 0,
      t_3_time_difference_in_days: 3.0,
      t_3_total_population: 113,
      t_3_population_increase: 0,
      t_3_population_increase_by_founded: 0,
      t_3_population_increase_by_conquered: 0,
      t_3_population_decrease: 0,
      t_3_population_decrease_by_conquered: 0,
      t_3_population_decrease_by_destroyed: 0,
      t_3_total_villages: 1,
      t_3_n_villages_with_population_increase: 0,
      t_3_n_villages_with_population_decrease: 0,
      t_3_n_villages_with_population_stuck: 0,
      t_3_new_village_founded: 0,
      t_3_new_village_conquered: 0,
      t_3_lost_village_conquered: 0,
      t_3_lost_village_destroyed: 0,
      t_4_has_data?: 0,
      t_4_has_increase?: 0,
      t_4_time_difference_in_days: 0,
      t_4_total_population: 0,
      t_4_population_increase: 0,
      t_4_population_increase_by_founded: 0,
      t_4_population_increase_by_conquered: 0,
      t_4_population_decrease: 0,
      t_4_population_decrease_by_conquered: 0,
      t_4_population_decrease_by_destroyed: 0,
      t_4_total_villages: 0,
      t_4_n_villages_with_population_increase: 0,
      t_4_n_villages_with_population_decrease: 0,
      t_4_n_villages_with_population_stuck: 0,
      t_4_new_village_founded: 0,
      t_4_new_village_conquered: 0,
      t_4_lost_village_conquered: 0,
      t_4_lost_village_destroyed: 0,
      t_5_has_data?: 0,
      t_5_has_increase?: 0,
      t_5_time_difference_in_days: 0,
      t_5_total_population: 0,
      t_5_population_increase: 0,
      t_5_population_increase_by_founded: 0,
      t_5_population_increase_by_conquered: 0,
      t_5_population_decrease: 0,
      t_5_population_decrease_by_conquered: 0,
      t_5_population_decrease_by_destroyed: 0,
      t_5_total_villages: 0,
      t_5_n_villages_with_population_increase: 0,
      t_5_n_villages_with_population_decrease: 0,
      t_5_n_villages_with_population_stuck: 0,
      t_5_new_village_founded: 0,
      t_5_new_village_conquered: 0,
      t_5_lost_village_conquered: 0,
      t_5_lost_village_destroyed: 0,
      t_6_has_data?: 0,
      t_6_has_increase?: 0,
      t_6_time_difference_in_days: 0,
      t_6_total_population: 0,
      t_6_population_increase: 0,
      t_6_population_increase_by_founded: 0,
      t_6_population_increase_by_conquered: 0,
      t_6_population_decrease: 0,
      t_6_population_decrease_by_conquered: 0,
      t_6_population_decrease_by_destroyed: 0,
      t_6_total_villages: 0,
      t_6_n_villages_with_population_increase: 0,
      t_6_n_villages_with_population_decrease: 0,
      t_6_n_villages_with_population_stuck: 0,
      t_6_new_village_founded: 0,
      t_6_new_village_conquered: 0,
      t_6_lost_village_conquered: 0,
      t_6_lost_village_destroyed: 0,
      t_7_has_data?: 0,
      t_7_has_increase?: 0,
      t_7_time_difference_in_days: 0,
      t_7_total_population: 0,
      t_7_population_increase: 0,
      t_7_population_increase_by_founded: 0,
      t_7_population_increase_by_conquered: 0,
      t_7_population_decrease: 0,
      t_7_population_decrease_by_conquered: 0,
      t_7_population_decrease_by_destroyed: 0,
      t_7_total_villages: 0,
      t_7_n_villages_with_population_increase: 0,
      t_7_n_villages_with_population_decrease: 0,
      t_7_n_villages_with_population_stuck: 0,
      t_7_new_village_founded: 0,
      t_7_new_village_conquered: 0,
      t_7_lost_village_conquered: 0,
      t_7_lost_village_destroyed: 0
    }

    active_agg_player = %Collector.AggPlayers{
      target_dt: ~U[2023-04-29 00:00:00.000Z],
      server_id: "https://ts1.x1.international.travian.com",
      player_id: "https://ts1.x1.international.travian.com--P--6319",
      estimated_starting_date: ~D[2023-04-26],
      estimated_tribe: 3,
      increment: [
        %Collector.AggPlayers.Increment{
          target_dt: ~U[2023-04-29 00:00:00.000Z],
          total_population: 258,
          population_increase: 5,
          population_increase_by_founded: 0,
          population_increase_by_conquered: 0,
          population_decrease: 0,
          population_decrease_by_conquered: 0,
          population_decrease_by_destroyed: 0,
          total_villages: 1,
          n_villages_with_population_increase: 1,
          n_villages_with_population_decrease: 0,
          n_villages_with_population_stuck: 0,
          new_village_founded: 0,
          new_village_conquered: 0,
          lost_village_conquered: 0,
          lost_village_destroyed: 0
        },
        %Collector.AggPlayers.Increment{
          target_dt: ~U[2023-04-28 00:00:00.000Z],
          total_population: 253,
          population_increase: 50,
          population_increase_by_founded: 0,
          population_increase_by_conquered: 0,
          population_decrease: 0,
          population_decrease_by_conquered: 0,
          population_decrease_by_destroyed: 0,
          total_villages: 1,
          n_villages_with_population_increase: 1,
          n_villages_with_population_decrease: 0,
          n_villages_with_population_stuck: 0,
          new_village_founded: 0,
          new_village_conquered: 0,
          lost_village_conquered: 0,
          lost_village_destroyed: 0
        },
        %Collector.AggPlayers.Increment{
          target_dt: ~U[2023-04-27 00:00:00.000Z],
          total_population: 203,
          population_increase: 90,
          population_increase_by_founded: 0,
          population_increase_by_conquered: 0,
          population_decrease: 0,
          population_decrease_by_conquered: 0,
          population_decrease_by_destroyed: 0,
          total_villages: 1,
          n_villages_with_population_increase: 1,
          n_villages_with_population_decrease: 0,
          n_villages_with_population_stuck: 0,
          new_village_founded: 0,
          new_village_conquered: 0,
          lost_village_conquered: 0,
          lost_village_destroyed: 0
        },
        %Collector.AggPlayers.Increment{
          target_dt: ~U[2023-04-26 00:00:00.000Z],
          total_population: 113,
          population_increase: nil,
          population_increase_by_founded: nil,
          population_increase_by_conquered: nil,
          population_decrease: nil,
          population_decrease_by_conquered: nil,
          population_decrease_by_destroyed: nil,
          total_villages: 1,
          n_villages_with_population_increase: nil,
          n_villages_with_population_decrease: nil,
          n_villages_with_population_stuck: nil,
          new_village_founded: nil,
          new_village_conquered: nil,
          lost_village_conquered: nil,
          lost_village_destroyed: nil
        }
      ]
    }

    expected_active_output = %Collector.MedusaTrain.Sample{
      sample: active_medusa_pred_input,
      labeling_dt: target_dt,
      is_inactive?: false
    }

    assert(
      expected_active_output ==
        Collector.MedusaTrain.process(target_dt, active_agg_player, active_medusa_pred_input)
    )

    inactive_medusa_pred_input = %Collector.MedusaPredInput{
      target_dt: ~U[2023-04-29 00:00:00.000Z],
      server_id: "https://ts1.x1.international.travian.com",
      player_id: "https://ts1.x1.international.travian.com--P--6319",
      has_alliance?: 1,
      server_days_from_start: 22,
      has_speed?: 1,
      speed: 1,
      player_days_from_start: 3,
      estimated_tribe: 3,
      t_has_increase?: 1,
      t_total_population: 253,
      t_population_increase: 0,
      t_population_increase_by_founded: 0,
      t_population_increase_by_conquered: 0,
      t_population_decrease: 0,
      t_population_decrease_by_conquered: 0,
      t_population_decrease_by_destroyed: 0,
      t_total_villages: 1,
      t_n_villages_with_population_increase: 0,
      t_n_villages_with_population_decrease: 0,
      t_n_villages_with_population_stuck: 1,
      t_new_village_founded: 0,
      t_new_village_conquered: 0,
      t_lost_village_conquered: 0,
      t_lost_village_destroyed: 0,
      t_1_has_data?: 1,
      t_1_has_increase?: 1,
      t_1_time_difference_in_days: 1.0,
      t_1_total_population: 253,
      t_1_population_increase: 50,
      t_1_population_increase_by_founded: 0,
      t_1_population_increase_by_conquered: 0,
      t_1_population_decrease: 0,
      t_1_population_decrease_by_conquered: 0,
      t_1_population_decrease_by_destroyed: 0,
      t_1_total_villages: 1,
      t_1_n_villages_with_population_increase: 1,
      t_1_n_villages_with_population_decrease: 0,
      t_1_n_villages_with_population_stuck: 0,
      t_1_new_village_founded: 0,
      t_1_new_village_conquered: 0,
      t_1_lost_village_conquered: 0,
      t_1_lost_village_destroyed: 0,
      t_2_has_data?: 1,
      t_2_has_increase?: 1,
      t_2_time_difference_in_days: 2.0,
      t_2_total_population: 203,
      t_2_population_increase: 90,
      t_2_population_increase_by_founded: 0,
      t_2_population_increase_by_conquered: 0,
      t_2_population_decrease: 0,
      t_2_population_decrease_by_conquered: 0,
      t_2_population_decrease_by_destroyed: 0,
      t_2_total_villages: 1,
      t_2_n_villages_with_population_increase: 1,
      t_2_n_villages_with_population_decrease: 0,
      t_2_n_villages_with_population_stuck: 0,
      t_2_new_village_founded: 0,
      t_2_new_village_conquered: 0,
      t_2_lost_village_conquered: 0,
      t_2_lost_village_destroyed: 0,
      t_3_has_data?: 1,
      t_3_has_increase?: 0,
      t_3_time_difference_in_days: 3.0,
      t_3_total_population: 113,
      t_3_population_increase: 0,
      t_3_population_increase_by_founded: 0,
      t_3_population_increase_by_conquered: 0,
      t_3_population_decrease: 0,
      t_3_population_decrease_by_conquered: 0,
      t_3_population_decrease_by_destroyed: 0,
      t_3_total_villages: 1,
      t_3_n_villages_with_population_increase: 0,
      t_3_n_villages_with_population_decrease: 0,
      t_3_n_villages_with_population_stuck: 0,
      t_3_new_village_founded: 0,
      t_3_new_village_conquered: 0,
      t_3_lost_village_conquered: 0,
      t_3_lost_village_destroyed: 0,
      t_4_has_data?: 0,
      t_4_has_increase?: 0,
      t_4_time_difference_in_days: 0,
      t_4_total_population: 0,
      t_4_population_increase: 0,
      t_4_population_increase_by_founded: 0,
      t_4_population_increase_by_conquered: 0,
      t_4_population_decrease: 0,
      t_4_population_decrease_by_conquered: 0,
      t_4_population_decrease_by_destroyed: 0,
      t_4_total_villages: 0,
      t_4_n_villages_with_population_increase: 0,
      t_4_n_villages_with_population_decrease: 0,
      t_4_n_villages_with_population_stuck: 0,
      t_4_new_village_founded: 0,
      t_4_new_village_conquered: 0,
      t_4_lost_village_conquered: 0,
      t_4_lost_village_destroyed: 0,
      t_5_has_data?: 0,
      t_5_has_increase?: 0,
      t_5_time_difference_in_days: 0,
      t_5_total_population: 0,
      t_5_population_increase: 0,
      t_5_population_increase_by_founded: 0,
      t_5_population_increase_by_conquered: 0,
      t_5_population_decrease: 0,
      t_5_population_decrease_by_conquered: 0,
      t_5_population_decrease_by_destroyed: 0,
      t_5_total_villages: 0,
      t_5_n_villages_with_population_increase: 0,
      t_5_n_villages_with_population_decrease: 0,
      t_5_n_villages_with_population_stuck: 0,
      t_5_new_village_founded: 0,
      t_5_new_village_conquered: 0,
      t_5_lost_village_conquered: 0,
      t_5_lost_village_destroyed: 0,
      t_6_has_data?: 0,
      t_6_has_increase?: 0,
      t_6_time_difference_in_days: 0,
      t_6_total_population: 0,
      t_6_population_increase: 0,
      t_6_population_increase_by_founded: 0,
      t_6_population_increase_by_conquered: 0,
      t_6_population_decrease: 0,
      t_6_population_decrease_by_conquered: 0,
      t_6_population_decrease_by_destroyed: 0,
      t_6_total_villages: 0,
      t_6_n_villages_with_population_increase: 0,
      t_6_n_villages_with_population_decrease: 0,
      t_6_n_villages_with_population_stuck: 0,
      t_6_new_village_founded: 0,
      t_6_new_village_conquered: 0,
      t_6_lost_village_conquered: 0,
      t_6_lost_village_destroyed: 0,
      t_7_has_data?: 0,
      t_7_has_increase?: 0,
      t_7_time_difference_in_days: 0,
      t_7_total_population: 0,
      t_7_population_increase: 0,
      t_7_population_increase_by_founded: 0,
      t_7_population_increase_by_conquered: 0,
      t_7_population_decrease: 0,
      t_7_population_decrease_by_conquered: 0,
      t_7_population_decrease_by_destroyed: 0,
      t_7_total_villages: 0,
      t_7_n_villages_with_population_increase: 0,
      t_7_n_villages_with_population_decrease: 0,
      t_7_n_villages_with_population_stuck: 0,
      t_7_new_village_founded: 0,
      t_7_new_village_conquered: 0,
      t_7_lost_village_conquered: 0,
      t_7_lost_village_destroyed: 0
    }

    inactive_agg_player = %Collector.AggPlayers{
      target_dt: ~U[2023-04-29 00:00:00.000Z],
      server_id: "https://ts1.x1.international.travian.com",
      player_id: "https://ts1.x1.international.travian.com--P--6319",
      estimated_starting_date: ~D[2023-04-26],
      estimated_tribe: 3,
      increment: [
        %Collector.AggPlayers.Increment{
          target_dt: ~U[2023-04-29 00:00:00.000Z],
          total_population: 253,
          population_increase: 0,
          population_increase_by_founded: 0,
          population_increase_by_conquered: 0,
          population_decrease: 0,
          population_decrease_by_conquered: 0,
          population_decrease_by_destroyed: 0,
          total_villages: 1,
          n_villages_with_population_increase: 0,
          n_villages_with_population_decrease: 0,
          n_villages_with_population_stuck: 1,
          new_village_founded: 0,
          new_village_conquered: 0,
          lost_village_conquered: 0,
          lost_village_destroyed: 0
        },
        %Collector.AggPlayers.Increment{
          target_dt: ~U[2023-04-28 00:00:00.000Z],
          total_population: 253,
          population_increase: 50,
          population_increase_by_founded: 0,
          population_increase_by_conquered: 0,
          population_decrease: 0,
          population_decrease_by_conquered: 0,
          population_decrease_by_destroyed: 0,
          total_villages: 1,
          n_villages_with_population_increase: 1,
          n_villages_with_population_decrease: 0,
          n_villages_with_population_stuck: 0,
          new_village_founded: 0,
          new_village_conquered: 0,
          lost_village_conquered: 0,
          lost_village_destroyed: 0
        },
        %Collector.AggPlayers.Increment{
          target_dt: ~U[2023-04-27 00:00:00.000Z],
          total_population: 203,
          population_increase: 90,
          population_increase_by_founded: 0,
          population_increase_by_conquered: 0,
          population_decrease: 0,
          population_decrease_by_conquered: 0,
          population_decrease_by_destroyed: 0,
          total_villages: 1,
          n_villages_with_population_increase: 1,
          n_villages_with_population_decrease: 0,
          n_villages_with_population_stuck: 0,
          new_village_founded: 0,
          new_village_conquered: 0,
          lost_village_conquered: 0,
          lost_village_destroyed: 0
        },
        %Collector.AggPlayers.Increment{
          target_dt: ~U[2023-04-26 00:00:00.000Z],
          total_population: 113,
          population_increase: nil,
          population_increase_by_founded: nil,
          population_increase_by_conquered: nil,
          population_decrease: nil,
          population_decrease_by_conquered: nil,
          population_decrease_by_destroyed: nil,
          total_villages: 1,
          n_villages_with_population_increase: nil,
          n_villages_with_population_decrease: nil,
          n_villages_with_population_stuck: nil,
          new_village_founded: nil,
          new_village_conquered: nil,
          lost_village_conquered: nil,
          lost_village_destroyed: nil
        }
      ]
    }

    expected_inactive_output = %Collector.MedusaTrain.Sample{
      sample: inactive_medusa_pred_input,
      labeling_dt: target_dt,
      is_inactive?: true
    }

    assert(
      expected_inactive_output ==
        Collector.MedusaTrain.process(target_dt, inactive_agg_player, inactive_medusa_pred_input)
    )
  end
end
