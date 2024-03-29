defmodule Collector.MedusaPredInputTest do
  use ExUnit.Case

  @tag :tmp_dir
  test "run fails if there is no snapshot", %{tmp_dir: root_folder} do
    target_date = Date.utc_today()
    server_id = "server1"
    {:error, {msg, _reason}} = Collector.MedusaPredInput.run(root_folder, server_id, target_date)
    assert(msg == "Unable to open snapshot")
  end

  @tag :tmp_dir
  test "run fails if there is no agg_player", %{tmp_dir: root_folder} do
    target_date = Date.utc_today()
    server_id = "server1"
    content = "blabla"
    :ok = Collector.Feed.store(root_folder, server_id, target_date, content, Collector.Snapshot)
    {:error, {msg, _reason}} = Collector.MedusaPredInput.run(root_folder, server_id, target_date)
    assert(msg == "Unable to open agg_players")
  end

  @tag :tmp_dir
  test "run fails if there is no agg_server", %{tmp_dir: root_folder} do
    target_date = Date.utc_today()
    server_id = "server1"
    content = "blabla"
    :ok = Collector.Feed.store(root_folder, server_id, target_date, content, Collector.Snapshot)
    :ok = Collector.Feed.store(root_folder, server_id, target_date, content, Collector.AggPlayers)
    {:error, {msg, _reason}} = Collector.MedusaPredInput.run(root_folder, server_id, target_date)
    assert(msg == "Unable to open agg_server")
  end

  @tag :tmp_dir
  test "run creates a prediction input feed", %{tmp_dir: root_folder} do
    server_id = "https://ts8.x1.international.travian.com"
    target_dt = ~U[2023-04-15 00:00:00.000Z]
    target_date = DateTime.to_date(target_dt)

    snapshot = [
      %Collector.Snapshot{
        map_id: 359,
        x: 158,
        y: 200,
        tribe: 3,
        village_id: "https://ts8.x1.international.travian.com--V--25384",
        village_server_id: 25384,
        village_name: "T 01",
        player_id: "https://ts8.x1.international.travian.com--P--5576",
        player_server_id: 5576,
        player_name: "kprincess",
        alliance_id: "https://ts8.x1.international.travian.com--A--299",
        alliance_server_id: 299,
        alliance_name: "Braz",
        population: 269,
        region: nil,
        is_capital: false,
        is_city: nil,
        has_harbor: nil,
        victory_points: nil
      },
      %Collector.Snapshot{
        map_id: 395,
        x: 194,
        y: 200,
        tribe: 3,
        village_id: "https://ts8.x1.international.travian.com--V--24398",
        village_server_id: 24398,
        village_name: "Le Perreux Sur Marne",
        player_id: "https://ts8.x1.international.travian.com--P--2051",
        player_server_id: 2051,
        player_name: "Fred4000",
        alliance_id: "https://ts8.x1.international.travian.com--A--60",
        alliance_server_id: 60,
        alliance_name: "TC+",
        population: 360,
        region: nil,
        is_capital: false,
        is_city: nil,
        has_harbor: nil,
        victory_points: nil
      }
    ]

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
      },
      %Collector.AggPlayers{
        target_dt: ~U[2023-04-15 00:00:00.000Z],
        server_id: "https://ts8.x1.international.travian.com",
        player_id: "https://ts8.x1.international.travian.com--P--5576",
        estimated_starting_date: ~D[2023-04-15],
        estimated_tribe: 3,
        increment: [
          %Collector.AggPlayers.Increment{
            target_dt: ~U[2023-04-15 00:00:00.000Z],
            total_population: 1032,
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

    agg_server = %Collector.AggServer{
      target_dt: ~U[2023-04-15 00:00:00.000Z],
      server_id: "https://ts8.x1.international.travian.com",
      url: "https://ts8.x1.international.travian.com",
      shrink: "ts8",
      speed: 1,
      region: "international",
      estimated_starting_date: ~D[2023-04-15],
      increment: [
        %Collector.AggServer.Increment{
          target_dt: ~U[2023-04-15 00:00:00.000Z],
          natar_villages: 503,
          natar_population: 66449,
          natar_population_variation: nil,
          total_population: 1_890_046,
          population_variation: nil,
          total_villages: 7226,
          new_villages: nil,
          removed_villages: nil,
          total_players: 4576,
          new_players: nil,
          removed_players: nil,
          total_alliances: 163,
          new_alliances: nil,
          removed_alliances: nil
        }
      ]
    }

    :ok = Collector.Feed.store(root_folder, server_id, target_date, snapshot, Collector.Snapshot)

    :ok =
      Collector.Feed.store(root_folder, server_id, target_date, agg_players, Collector.AggPlayers)

    :ok =
      Collector.Feed.store(root_folder, server_id, target_date, agg_server, Collector.AggServer)

    :ok = Collector.MedusaPredInput.run(root_folder, server_id, target_date)

    {:ok, medusa_pred_input} =
      Collector.Feed.open(root_folder, server_id, target_date, Collector.MedusaPredInput)

    assert(length(medusa_pred_input) == 2)
    [p1, p2] = Enum.sort_by(medusa_pred_input, & &1.player_id)

    assert(p1.target_dt == target_dt)
    assert(p1.server_id == server_id)
    assert(p1.has_alliance? == 1)
    assert(p1.server_days_from_start == 0)
    assert(p1.has_speed? == 1)
    assert(p1.speed == 1)
    assert(p1.player_days_from_start == 0)
    assert(p1.estimated_tribe == 3)
    assert(p1.t_has_increase? == 0)
    assert(p1.t_total_population == 1047)
    assert(p1.t_total_villages == 3)

    assert(p2.target_dt == target_dt)
    assert(p2.server_id == server_id)
    assert(p2.has_alliance? == 1)
    assert(p2.server_days_from_start == 0)
    assert(p2.has_speed? == 1)
    assert(p2.speed == 1)
    assert(p2.player_days_from_start == 0)
    assert(p2.estimated_tribe == 3)
    assert(p2.t_has_increase? == 0)
    assert(p2.t_total_population == 1032)
    assert(p2.t_total_villages == 3)
  end

  @tag :tmp_dir
  test "run doesn't use removed players", %{tmp_dir: root_folder} do
    server_id = "https://ts8.x1.international.travian.com"
    target_dt = ~U[2023-04-15 00:00:00.000Z]
    target_date = DateTime.to_date(target_dt)

    snapshot = [
      %Collector.Snapshot{
        map_id: 359,
        x: 158,
        y: 200,
        tribe: 3,
        village_id: "https://ts8.x1.international.travian.com--V--25384",
        village_server_id: 25384,
        village_name: "T 01",
        player_id: "https://ts8.x1.international.travian.com--P--5576",
        player_server_id: 5576,
        player_name: "kprincess",
        alliance_id: "https://ts8.x1.international.travian.com--A--299",
        alliance_server_id: 299,
        alliance_name: "Braz",
        population: 269,
        region: nil,
        is_capital: false,
        is_city: nil,
        has_harbor: nil,
        victory_points: nil
      }
    ]

    agg_players = [
      %Collector.AggPlayers{
        target_dt: ~U[2023-04-14 00:00:00.000Z],
        server_id: "https://ts8.x1.international.travian.com",
        player_id: "https://ts8.x1.international.travian.com--P--2051",
        estimated_starting_date: ~D[2023-04-15],
        estimated_tribe: 3,
        increment: [
          %Collector.AggPlayers.Increment{
            target_dt: ~U[2023-04-14 00:00:00.000Z],
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
      },
      %Collector.AggPlayers{
        target_dt: ~U[2023-04-15 00:00:00.000Z],
        server_id: "https://ts8.x1.international.travian.com",
        player_id: "https://ts8.x1.international.travian.com--P--5576",
        estimated_starting_date: ~D[2023-04-15],
        estimated_tribe: 3,
        increment: [
          %Collector.AggPlayers.Increment{
            target_dt: ~U[2023-04-15 00:00:00.000Z],
            total_population: 1032,
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

    agg_server = %Collector.AggServer{
      target_dt: ~U[2023-04-15 00:00:00.000Z],
      server_id: "https://ts8.x1.international.travian.com",
      url: "https://ts8.x1.international.travian.com",
      shrink: "ts8",
      speed: 1,
      region: "international",
      estimated_starting_date: ~D[2023-04-14],
      increment: [
        %Collector.AggServer.Increment{
          target_dt: ~U[2023-04-15 00:00:00.000Z],
          natar_villages: 503,
          natar_population: 66449,
          natar_population_variation: nil,
          total_population: 1_890_046,
          population_variation: nil,
          total_villages: 7226,
          new_villages: nil,
          removed_villages: nil,
          total_players: 4576,
          new_players: nil,
          removed_players: nil,
          total_alliances: 163,
          new_alliances: nil,
          removed_alliances: nil
        }
      ]
    }

    :ok = Collector.Feed.store(root_folder, server_id, target_date, snapshot, Collector.Snapshot)

    :ok =
      Collector.Feed.store(root_folder, server_id, target_date, agg_players, Collector.AggPlayers)

    :ok =
      Collector.Feed.store(root_folder, server_id, target_date, agg_server, Collector.AggServer)

    :ok = Collector.MedusaPredInput.run(root_folder, server_id, target_date)

    {:ok, medusa_pred_input} =
      Collector.Feed.open(root_folder, server_id, target_date, Collector.MedusaPredInput)

    assert(length(medusa_pred_input) == 1)
    [p1] = medusa_pred_input

    assert(p1.player_id == "https://ts8.x1.international.travian.com--P--5576")
    assert(p1.target_dt == target_dt)
    assert(p1.server_id == server_id)
    assert(p1.has_alliance? == 1)
    assert(p1.server_days_from_start == 1)
    assert(p1.has_speed? == 1)
    assert(p1.speed == 1)
    assert(p1.player_days_from_start == 0)
    assert(p1.estimated_tribe == 3)
    assert(p1.t_has_increase? == 0)
    assert(p1.t_total_population == 1032)
    assert(p1.t_total_villages == 3)
  end

  @tag :tmp_dir
  test "process fills the nil values with 0 when there is only 1 increment", %{
    tmp_dir: root_folder
  } do
    server_id = "https://ts39.x3.international.travian.com"
    target_dt = ~U[2022-10-16 00:00:00.000Z]
    target_date = DateTime.to_date(target_dt)

    snapshot = [
      %Collector.Snapshot{
        map_id: 68357,
        x: -14,
        y: 30,
        tribe: 3,
        village_id: "https://ts39.x3.international.travian.com--V--17395",
        village_server_id: 17395,
        village_name: "*",
        player_id: "https://ts39.x3.international.travian.com--P--129",
        player_server_id: 129,
        player_name: "Ferrus",
        alliance_id: "https://ts39.x3.international.travian.com--A--0",
        alliance_server_id: 0,
        alliance_name: "",
        population: 112,
        region: nil,
        is_capital: true,
        is_city: nil,
        has_harbor: nil,
        victory_points: nil
      }
    ]

    agg_players = [
      %Collector.AggPlayers{
        target_dt: ~U[2022-10-14 00:00:00.000Z],
        server_id: "https://ts39.x3.international.travian.com",
        player_id: "https://ts39.x3.international.travian.com--P--129",
        estimated_starting_date: ~D[2022-10-14],
        estimated_tribe: 3,
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
      }
    ]

    agg_server = %Collector.AggServer{
      target_dt: ~U[2022-10-16 00:00:00.000Z],
      server_id: "https://ts39.x3.international.travian.com",
      url: "https://ts39.x3.international.travian.com",
      shrink: "ts39",
      speed: 3,
      region: "international",
      estimated_starting_date: ~D[2022-10-11],
      increment: [
        %Collector.AggServer.Increment{
          target_dt: ~U[2022-10-16 00:00:00.000Z],
          natar_villages: 14,
          natar_population: 3948,
          natar_population_variation: 0,
          total_population: 145_031,
          population_variation: 50532,
          total_villages: 1278,
          new_villages: 303,
          removed_villages: 0,
          total_players: 1178,
          new_players: 222,
          removed_players: 0,
          total_alliances: 25,
          new_alliances: 7,
          removed_alliances: 0
        }
      ]
    }

    :ok = Collector.Feed.store(root_folder, server_id, target_date, snapshot, Collector.Snapshot)

    :ok =
      Collector.Feed.store(root_folder, server_id, target_date, agg_players, Collector.AggPlayers)

    :ok =
      Collector.Feed.store(root_folder, server_id, target_date, agg_server, Collector.AggServer)

    :ok = Collector.MedusaPredInput.run(root_folder, server_id, target_date)

    {:ok, medusa_pred_input} =
      Collector.Feed.open(root_folder, server_id, target_date, Collector.MedusaPredInput)

    assert(length(medusa_pred_input) == 1)
    [p1] = medusa_pred_input

    assert(p1.player_id == "https://ts39.x3.international.travian.com--P--129")
    assert(p1.target_dt == target_dt)
    assert(p1.server_id == server_id)

    assert(p1.t_population_increase == 0)
    assert(p1.t_population_increase_by_founded == 0)
    assert(p1.t_population_increase_by_conquered == 0)
    assert(p1.t_population_decrease == 0)
    assert(p1.t_population_decrease_by_conquered == 0)
    assert(p1.t_population_decrease_by_destroyed == 0)
    assert(p1.t_n_villages_with_population_increase == 0)
    assert(p1.t_n_villages_with_population_decrease == 0)
    assert(p1.t_n_villages_with_population_stuck == 0)
    assert(p1.t_new_village_founded == 0)
    assert(p1.t_new_village_conquered == 0)
    assert(p1.t_lost_village_conquered == 0)
    assert(p1.t_lost_village_destroyed == 0)
  end

  @tag :tmp_dir
  test "process fills the N sample when there is an N increment avilable, otherwise, default value with :t_N_has_data? as 0",
       %{tmp_dir: root_folder} do
    server_id = "https://ts39.x3.international.travian.com"
    target_dt = ~U[2022-10-15 00:00:00.000Z]
    target_date = DateTime.to_date(target_dt)

    snapshot = [
      %Collector.Snapshot{
        map_id: 68357,
        x: -14,
        y: 30,
        tribe: 3,
        village_id: "https://ts39.x3.international.travian.com--V--17395",
        village_server_id: 17395,
        village_name: "*",
        player_id: "https://ts39.x3.international.travian.com--P--129",
        player_server_id: 129,
        player_name: "Ferrus",
        alliance_id: "https://ts39.x3.international.travian.com--A--14",
        alliance_server_id: 14,
        alliance_name: "Ęłitę",
        population: 197,
        region: nil,
        is_capital: true,
        is_city: nil,
        has_harbor: nil,
        victory_points: nil
      }
    ]

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

    agg_server = %Collector.AggServer{
      target_dt: ~U[2022-10-15 00:00:00.000Z],
      server_id: "https://ts39.x3.international.travian.com",
      url: "https://ts39.x3.international.travian.com",
      shrink: "ts39",
      speed: 3,
      region: "international",
      estimated_starting_date: ~D[2022-10-11],
      increment: [
        %Collector.AggServer.Increment{
          target_dt: ~U[2022-10-15 00:00:00.000Z],
          natar_villages: 14,
          natar_population: 3948,
          natar_population_variation: 0,
          total_population: 145_031,
          population_variation: 50532,
          total_villages: 1278,
          new_villages: 303,
          removed_villages: 0,
          total_players: 1178,
          new_players: 222,
          removed_players: 0,
          total_alliances: 25,
          new_alliances: 7,
          removed_alliances: 0
        }
      ]
    }

    :ok = Collector.Feed.store(root_folder, server_id, target_date, snapshot, Collector.Snapshot)

    :ok =
      Collector.Feed.store(root_folder, server_id, target_date, agg_players, Collector.AggPlayers)

    :ok =
      Collector.Feed.store(root_folder, server_id, target_date, agg_server, Collector.AggServer)

    :ok = Collector.MedusaPredInput.run(root_folder, server_id, target_date)

    {:ok, medusa_pred_input} =
      Collector.Feed.open(root_folder, server_id, target_date, Collector.MedusaPredInput)

    assert(length(medusa_pred_input) == 1)
    [p1] = medusa_pred_input

    assert(p1.player_id == "https://ts39.x3.international.travian.com--P--129")
    assert(p1.target_dt == target_dt)
    assert(p1.server_id == server_id)

    assert(p1.t_has_increase? == 1)
    assert(p1.t_1_has_data? == 1)
    assert(p1.t_2_has_data? == 0)
    assert(p1.t_3_has_data? == 0)
    assert(p1.t_4_has_data? == 0)
    assert(p1.t_5_has_data? == 0)
    assert(p1.t_6_has_data? == 0)
    assert(p1.t_7_has_data? == 0)
  end

  @tag :tmp_dir
  test "if there is a previous increment available, the :t_N_has_increase? flag should be 1", %{
    tmp_dir: root_folder
  } do
    server_id = "https://ts39.x3.international.travian.com"
    target_dt = ~U[2022-10-16 00:00:00.000Z]
    target_date = DateTime.to_date(target_dt)

    snapshot = [
      %Collector.Snapshot{
        map_id: 66715,
        x: -52,
        y: 34,
        tribe: 3,
        village_id: "https://ts39.x3.international.travian.com--V--18294",
        village_server_id: 18294,
        village_name: "00",
        player_id: "https://ts39.x3.international.travian.com--P--129",
        player_server_id: 129,
        player_name: "Ferrus",
        alliance_id: "https://ts39.x3.international.travian.com--A--14",
        alliance_server_id: 14,
        alliance_name: "Ęłitę",
        population: 68,
        region: nil,
        is_capital: false,
        is_city: nil,
        has_harbor: nil,
        victory_points: nil
      }
    ]

    agg_players = [
      %Collector.AggPlayers{
        target_dt: ~U[2022-10-16 00:00:00.000Z],
        server_id: "https://ts39.x3.international.travian.com",
        player_id: "https://ts39.x3.international.travian.com--P--129",
        estimated_starting_date: ~D[2022-10-14],
        estimated_tribe: 3,
        increment: [
          %Collector.AggPlayers.Increment{
            target_dt: ~U[2022-10-16 00:00:00.000Z],
            total_population: 329,
            population_increase: 64,
            population_increase_by_founded: 68,
            population_increase_by_conquered: 0,
            population_decrease: 0,
            population_decrease_by_conquered: 0,
            population_decrease_by_destroyed: 0,
            total_villages: 2,
            n_villages_with_population_increase: 1,
            n_villages_with_population_decrease: 0,
            n_villages_with_population_stuck: 0,
            new_village_founded: 1,
            new_village_conquered: 0,
            lost_village_conquered: 0,
            lost_village_destroyed: 0
          },
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

    agg_server = %Collector.AggServer{
      target_dt: ~U[2022-10-15 00:00:00.000Z],
      server_id: "https://ts39.x3.international.travian.com",
      url: "https://ts39.x3.international.travian.com",
      shrink: "ts39",
      speed: 3,
      region: "international",
      estimated_starting_date: ~D[2022-10-11],
      increment: [
        %Collector.AggServer.Increment{
          target_dt: ~U[2022-10-15 00:00:00.000Z],
          natar_villages: 14,
          natar_population: 3948,
          natar_population_variation: 0,
          total_population: 145_031,
          population_variation: 50532,
          total_villages: 1278,
          new_villages: 303,
          removed_villages: 0,
          total_players: 1178,
          new_players: 222,
          removed_players: 0,
          total_alliances: 25,
          new_alliances: 7,
          removed_alliances: 0
        }
      ]
    }

    :ok = Collector.Feed.store(root_folder, server_id, target_date, snapshot, Collector.Snapshot)

    :ok =
      Collector.Feed.store(root_folder, server_id, target_date, agg_players, Collector.AggPlayers)

    :ok =
      Collector.Feed.store(root_folder, server_id, target_date, agg_server, Collector.AggServer)

    :ok = Collector.MedusaPredInput.run(root_folder, server_id, target_date)

    {:ok, medusa_pred_input} =
      Collector.Feed.open(root_folder, server_id, target_date, Collector.MedusaPredInput)

    assert(length(medusa_pred_input) == 1)
    [p1] = medusa_pred_input

    assert(p1.player_id == "https://ts39.x3.international.travian.com--P--129")
    assert(p1.target_dt == target_dt)
    assert(p1.server_id == server_id)

    assert(p1.t_1_has_data? == 1)
    assert(p1.t_2_has_data? == 1)
    assert(p1.t_3_has_data? == 0)
    assert(p1.t_4_has_data? == 0)
    assert(p1.t_5_has_data? == 0)
    assert(p1.t_6_has_data? == 0)
    assert(p1.t_7_has_data? == 0)

    assert(p1.t_has_increase? == 1)
    assert(p1.t_1_has_increase? == 1)
    assert(p1.t_2_has_increase? == 0)
    assert(p1.t_3_has_increase? == 0)
    assert(p1.t_4_has_increase? == 0)
    assert(p1.t_5_has_increase? == 0)
    assert(p1.t_6_has_increase? == 0)
    assert(p1.t_7_has_increase? == 0)
  end

  @tag :tmp_dir
  test "if there are more than 7 samples, only use the last 7", %{tmp_dir: root_folder} do
    server_id = "https://ts39.x3.international.travian.com"
    target_dt = ~U[2022-11-07 00:00:00.000Z]
    target_date = DateTime.to_date(target_dt)

    snapshot = [
      %Collector.Snapshot{
        map_id: 66314,
        x: -52,
        y: 35,
        tribe: 3,
        village_id: "https://ts39.x3.international.travian.com--V--19823",
        village_server_id: 19823,
        village_name: "02",
        player_id: "https://ts39.x3.international.travian.com--P--129",
        player_server_id: 129,
        player_name: "Ferrus",
        alliance_id: "https://ts39.x3.international.travian.com--A--0",
        alliance_server_id: 0,
        alliance_name: "",
        population: 165,
        region: nil,
        is_capital: false,
        is_city: nil,
        has_harbor: nil,
        victory_points: nil
      }
    ]

    agg_players = [
      %Collector.AggPlayers{
        target_dt: ~U[2022-11-07 00:00:00.000Z],
        server_id: "https://ts39.x3.international.travian.com",
        player_id: "https://ts39.x3.international.travian.com--P--129",
        estimated_starting_date: ~D[2022-10-14],
        estimated_tribe: 3,
        increment: [
          %Collector.AggPlayers.Increment{
            target_dt: ~U[2022-11-07 00:00:00.000Z],
            total_population: 1081,
            population_increase: 0,
            population_increase_by_founded: 0,
            population_increase_by_conquered: 0,
            population_decrease: 10,
            population_decrease_by_conquered: 0,
            population_decrease_by_destroyed: 0,
            total_villages: 4,
            n_villages_with_population_increase: 0,
            n_villages_with_population_decrease: 1,
            n_villages_with_population_stuck: 3,
            new_village_founded: 0,
            new_village_conquered: 0,
            lost_village_conquered: 0,
            lost_village_destroyed: 0
          },
          %Collector.AggPlayers.Increment{
            target_dt: ~U[2022-11-06 00:00:00.000Z],
            total_population: 1091,
            population_increase: 564,
            population_increase_by_founded: 229,
            population_increase_by_conquered: 0,
            population_decrease: 0,
            population_decrease_by_conquered: 499,
            population_decrease_by_destroyed: 0,
            total_villages: 4,
            n_villages_with_population_increase: 2,
            n_villages_with_population_decrease: 0,
            n_villages_with_population_stuck: 0,
            new_village_founded: 2,
            new_village_conquered: 0,
            lost_village_conquered: 1,
            lost_village_destroyed: 0
          },
          %Collector.AggPlayers.Increment{
            target_dt: ~U[2022-10-20 00:00:00.000Z],
            total_population: 751,
            population_increase: 114,
            population_increase_by_founded: 65,
            population_increase_by_conquered: 0,
            population_decrease: 0,
            population_decrease_by_conquered: 0,
            population_decrease_by_destroyed: 0,
            total_villages: 3,
            n_villages_with_population_increase: 2,
            n_villages_with_population_decrease: 0,
            n_villages_with_population_stuck: 0,
            new_village_founded: 1,
            new_village_conquered: 0,
            lost_village_conquered: 0,
            lost_village_destroyed: 0
          },
          %Collector.AggPlayers.Increment{
            target_dt: ~U[2022-10-19 00:00:00.000Z],
            total_population: 572,
            population_increase: 67,
            population_increase_by_founded: 0,
            population_increase_by_conquered: 0,
            population_decrease: 0,
            population_decrease_by_conquered: 0,
            population_decrease_by_destroyed: 0,
            total_villages: 2,
            n_villages_with_population_increase: 2,
            n_villages_with_population_decrease: 0,
            n_villages_with_population_stuck: 0,
            new_village_founded: 0,
            new_village_conquered: 0,
            lost_village_conquered: 0,
            lost_village_destroyed: 0
          },
          %Collector.AggPlayers.Increment{
            target_dt: ~U[2022-10-18 00:00:00.000Z],
            total_population: 505,
            population_increase: 76,
            population_increase_by_founded: 0,
            population_increase_by_conquered: 0,
            population_decrease: 0,
            population_decrease_by_conquered: 0,
            population_decrease_by_destroyed: 0,
            total_villages: 2,
            n_villages_with_population_increase: 2,
            n_villages_with_population_decrease: 0,
            n_villages_with_population_stuck: 0,
            new_village_founded: 0,
            new_village_conquered: 0,
            lost_village_conquered: 0,
            lost_village_destroyed: 0
          },
          %Collector.AggPlayers.Increment{
            target_dt: ~U[2022-10-17 00:00:00.000Z],
            total_population: 429,
            population_increase: 100,
            population_increase_by_founded: 0,
            population_increase_by_conquered: 0,
            population_decrease: 0,
            population_decrease_by_conquered: 0,
            population_decrease_by_destroyed: 0,
            total_villages: 2,
            n_villages_with_population_increase: 2,
            n_villages_with_population_decrease: 0,
            n_villages_with_population_stuck: 0,
            new_village_founded: 0,
            new_village_conquered: 0,
            lost_village_conquered: 0,
            lost_village_destroyed: 0
          },
          %Collector.AggPlayers.Increment{
            target_dt: ~U[2022-10-16 00:00:00.000Z],
            total_population: 329,
            population_increase: 64,
            population_increase_by_founded: 68,
            population_increase_by_conquered: 0,
            population_decrease: 0,
            population_decrease_by_conquered: 0,
            population_decrease_by_destroyed: 0,
            total_villages: 2,
            n_villages_with_population_increase: 1,
            n_villages_with_population_decrease: 0,
            n_villages_with_population_stuck: 0,
            new_village_founded: 1,
            new_village_conquered: 0,
            lost_village_conquered: 0,
            lost_village_destroyed: 0
          },
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

    agg_server = %Collector.AggServer{
      target_dt: ~U[2022-11-07 00:00:00.000Z],
      server_id: "https://ts39.x3.international.travian.com",
      url: "https://ts39.x3.international.travian.com",
      shrink: "ts39",
      speed: 3,
      region: "international",
      estimated_starting_date: ~D[2022-10-11],
      increment: [
        %Collector.AggServer.Increment{
          target_dt: ~U[2022-11-07 00:00:00.000Z],
          natar_villages: 14,
          natar_population: 3948,
          natar_population_variation: 0,
          total_population: 145_031,
          population_variation: 50532,
          total_villages: 1278,
          new_villages: 303,
          removed_villages: 0,
          total_players: 1178,
          new_players: 222,
          removed_players: 0,
          total_alliances: 25,
          new_alliances: 7,
          removed_alliances: 0
        }
      ]
    }

    :ok = Collector.Feed.store(root_folder, server_id, target_date, snapshot, Collector.Snapshot)

    :ok =
      Collector.Feed.store(root_folder, server_id, target_date, agg_players, Collector.AggPlayers)

    :ok =
      Collector.Feed.store(root_folder, server_id, target_date, agg_server, Collector.AggServer)

    :ok = Collector.MedusaPredInput.run(root_folder, server_id, target_date)

    {:ok, medusa_pred_input} =
      Collector.Feed.open(root_folder, server_id, target_date, Collector.MedusaPredInput)

    assert(length(medusa_pred_input) == 1)
    [p1] = medusa_pred_input

    assert(p1.player_id == "https://ts39.x3.international.travian.com--P--129")
    assert(p1.target_dt == target_dt)
    assert(p1.server_id == server_id)

    assert(p1.t_1_time_difference_in_days == 1)
    assert(p1.t_2_time_difference_in_days == 18)
    assert(p1.t_3_time_difference_in_days == 19)
    assert(p1.t_4_time_difference_in_days == 20)
    assert(p1.t_5_time_difference_in_days == 21)
    assert(p1.t_6_time_difference_in_days == 22)
    assert(p1.t_7_time_difference_in_days == 23)
  end
end
