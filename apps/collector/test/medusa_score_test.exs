defmodule Collector.MedusaScoreTest do
  use ExUnit.Case

  @tag :tmp_dir
  test "run fails if there is no agg_player of target_date", %{tmp_dir: root_folder} do
    target_date = Date.utc_today()
    server_id = "server1"

    {:error, {msg, _reason}} = Collector.MedusaScore.run(root_folder, server_id, target_date)

    assert(msg == "Unable to open agg_players")
  end

  @tag :tmp_dir
  test "run doesn't fail if there is no medusa_pred_output of target_date -1", %{
    tmp_dir: root_folder
  } do
    target_date = Date.utc_today()
    server_id = "server1"

    agg_players = [
      %Collector.AggPlayers{
        target_dt: ~U[2023-04-15 00:00:00.000Z],
        server_id: "https://ts8.x1.international.travian.com",
        player_id: "https://ts8.x1.international.travian.com--P--2051",
        estimated_starting_date: ~D[2023-04-15],
        estimated_tribe: 3,
        increment: [
          %Collector.AggPlayers.Increment{
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
        ]
      }
    ]

    :ok =
      Collector.Feed.store(root_folder, server_id, target_date, agg_players, Collector.AggPlayers)

    assert(:ok == Collector.MedusaScore.run(root_folder, server_id, target_date))
  end

  @tag :tmp_dir
  test "returns if the predicted value was right", %{tmp_dir: root_folder} do
    target_date = ~D[2022-10-15]
    server_id = "https://ts39.x3.international.travian.com"
    player_id = "https://ts39.x3.international.travian.com--P--129"

    agg_players = [
      %Collector.AggPlayers{
        target_dt: ~U[2022-10-15 00:00:00.000Z],
        server_id: "https://ts39.x3.international.travian.com",
        player_id: "https://ts39.x3.international.travian.com--P--129",
        estimated_starting_date: ~D[2022-10-14],
        estimated_tribe: 3,
        increment: [
          %Collector.AggPlayers.Increment{
            target_dt: ~U[2022-10-15 00:00:00.000Z],
            total_population: 197,
            population_increase: 85,
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
            target_dt: ~U[2022-10-14 00:00:00.000Z],
            total_population: 112,
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
    ]

    medusa_pred_output = [
      %Collector.MedusaPredOutput{
        target_dt: target_date,
        server_id: server_id,
        player_id: player_id,
        prediction: true,
        probability: 0.83
      }
    ]

    :ok =
      Collector.Feed.store(root_folder, server_id, target_date, agg_players, Collector.AggPlayers)

    :ok =
      Collector.Feed.store(
        root_folder,
        server_id,
        Date.add(target_date, -1),
        medusa_pred_output,
        Collector.MedusaPredOutput
      )

    :ok = Collector.MedusaScore.run(root_folder, server_id, target_date)

    {:ok, medusa_score} =
      Collector.Feed.open(root_folder, server_id, target_date, Collector.MedusaScore)

    assert(length(medusa_score) == 1)
    [player_score] = medusa_score
    assert(player_score.score == :false_positive)
    assert(player_score.probability == 0.83)
  end

  @tag :tmp_dir
  test "new and removed players in target_date are not considered", %{tmp_dir: root_folder} do
    target_date = ~D[2022-10-15]
    server_id = "https://ts39.x3.international.travian.com"

    agg_players = [
      %Collector.AggPlayers{
        target_dt: ~U[2022-10-15 00:00:00.000Z],
        server_id: "https://ts39.x3.international.travian.com",
        player_id: "https://ts39.x3.international.travian.com--P--129",
        estimated_starting_date: ~D[2022-10-14],
        estimated_tribe: 3,
        increment: [
          %Collector.AggPlayers.Increment{
            target_dt: ~U[2022-10-15 00:00:00.000Z],
            total_population: 197,
            population_increase: 85,
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
            target_dt: ~U[2022-10-14 00:00:00.000Z],
            total_population: 112,
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
      },
      %Collector.AggPlayers{
        target_dt: ~U[2022-10-14 00:00:00.000Z],
        server_id: "https://ts39.x3.international.travian.com",
        player_id: "https://ts39.x3.international.travian.com--P--130",
        estimated_starting_date: ~D[2022-10-14],
        estimated_tribe: 2,
        increment: [
          %Collector.AggPlayers.Increment{
            target_dt: ~U[2022-10-14 00:00:00.000Z],
            total_population: 112,
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
      },
      %Collector.AggPlayers{
        target_dt: ~U[2022-10-15 00:00:00.000Z],
        server_id: "https://ts39.x3.international.travian.com",
        player_id: "https://ts39.x3.international.travian.com--P--131",
        estimated_starting_date: ~D[2022-10-15],
        estimated_tribe: 1,
        increment: [
          %Collector.AggPlayers.Increment{
            target_dt: ~U[2022-10-15 00:00:00.000Z],
            total_population: 112,
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
    ]

    medusa_pred_output = [
      %Collector.MedusaPredOutput{
        target_dt: target_date,
        server_id: server_id,
        player_id: "https://ts39.x3.international.travian.com--P--129",
        prediction: true,
        probability: 0.83
      },
      %Collector.MedusaPredOutput{
        target_dt: target_date,
        server_id: server_id,
        player_id: "https://ts39.x3.international.travian.com--P--130",
        prediction: true,
        probability: 0.94
      }
    ]

    :ok =
      Collector.Feed.store(root_folder, server_id, target_date, agg_players, Collector.AggPlayers)

    :ok =
      Collector.Feed.store(
        root_folder,
        server_id,
        Date.add(target_date, -1),
        medusa_pred_output,
        Collector.MedusaPredOutput
      )

    :ok = Collector.MedusaScore.run(root_folder, server_id, target_date)

    {:ok, medusa_score} =
      Collector.Feed.open(root_folder, server_id, target_date, Collector.MedusaScore)

    assert(length(medusa_score) == 1)
    assert(hd(medusa_score).player_id == "https://ts39.x3.international.travian.com--P--129")
  end
end
