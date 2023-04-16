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
  %Collector.Snapshot{map_id: 359, x: 158, y: 200, tribe: 3, village_id: "https://ts8.x1.international.travian.com--V--25384", village_server_id: 25384, village_name: "T 01", player_id: "https://ts8.x1.international.travian.com--P--5576", player_server_id: 5576, player_name: "kprincess", alliance_id: "https://ts8.x1.international.travian.com--A--299", alliance_server_id: 299, alliance_name: "Braz", population: 269, region: nil, is_capital: false, is_city: nil, victory_points: nil},
  %Collector.Snapshot{map_id: 395, x: 194, y: 200, tribe: 3, village_id: "https://ts8.x1.international.travian.com--V--24398", village_server_id: 24398, village_name: "Le Perreux Sur Marne", player_id: "https://ts8.x1.international.travian.com--P--2051", player_server_id: 2051, player_name: "Fred4000", alliance_id: "https://ts8.x1.international.travian.com--A--60", alliance_server_id: 60, alliance_name: "TC+", population: 360, region: nil, is_capital: false, is_city: nil, victory_points: nil}
]
agg_players = [
  %Collector.AggPlayers{target_dt: ~U[2023-04-15 00:00:00.000Z], server_id: "https://ts8.x1.international.travian.com", player_id: "https://ts8.x1.international.travian.com--P--2051", estimated_starting_date: ~D[2023-04-15], estimated_tribe: 3, increment: [%Collector.AggPlayers.Increment{target_dt: ~U[2023-04-15 00:00:00.000Z], total_population: 1047, population_increase: nil, population_increase_by_founded: nil, population_increase_by_conquered: nil, population_decrease: nil, population_decrease_by_conquered: nil, population_decrease_by_destroyed: nil, total_villages: 3, n_villages_with_population_increase: nil, n_villages_with_population_decrease: nil, n_villages_with_population_stuck: nil, new_village_founded: nil, new_village_conquered: nil, lost_village_conquered: nil, lost_village_destroyed: nil}]},
  %Collector.AggPlayers{target_dt: ~U[2023-04-15 00:00:00.000Z], server_id: "https://ts8.x1.international.travian.com", player_id: "https://ts8.x1.international.travian.com--P--5576", estimated_starting_date: ~D[2023-04-15], estimated_tribe: 3, increment: [%Collector.AggPlayers.Increment{target_dt: ~U[2023-04-15 00:00:00.000Z], total_population: 1032, population_increase: nil, population_increase_by_founded: nil, population_increase_by_conquered: nil, population_decrease: nil, population_decrease_by_conquered: nil, population_decrease_by_destroyed: nil, total_villages: 3, n_villages_with_population_increase: nil, n_villages_with_population_decrease: nil, n_villages_with_population_stuck: nil, new_village_founded: nil, new_village_conquered: nil, lost_village_conquered: nil, lost_village_destroyed: nil}]}
]

agg_server = %Collector.AggServer{target_dt: ~U[2023-04-15 00:00:00.000Z], server_id: "https://ts8.x1.international.travian.com", url: "https://ts8.x1.international.travian.com", shrink: "ts8", speed: 1, region: "international", estimated_starting_date: ~D[2023-04-15], increment: [%Collector.AggServer.Increment{target_dt: ~U[2023-04-15 00:00:00.000Z], natar_villages: 503, natar_population: 66449, natar_population_variation: nil, total_population: 1890046, population_variation: nil, total_villages: 7226, new_villages: nil, removed_villages: nil, total_players: 4576, new_players: nil, removed_players: nil, total_alliances: 163, new_alliances: nil, removed_alliances: nil}]}

    :ok = Collector.Feed.store(root_folder, server_id, target_date, snapshot, Collector.Snapshot)
    :ok = Collector.Feed.store(root_folder, server_id, target_date, agg_players, Collector.AggPlayers)
    :ok = Collector.Feed.store(root_folder, server_id, target_date, agg_server, Collector.AggServer)

    :ok = Collector.MedusaPredInput.run(root_folder, server_id, target_date)
    {:ok, medusa_pred_input} = Collector.Feed.open(root_folder, server_id, target_date, Collector.MedusaPredInput)

    assert(length(medusa_pred_input) == 2)
    [p1, p2] = Enum.sort_by(medusa_pred_input, &(&1.player_id))

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
  %Collector.Snapshot{map_id: 359, x: 158, y: 200, tribe: 3, village_id: "https://ts8.x1.international.travian.com--V--25384", village_server_id: 25384, village_name: "T 01", player_id: "https://ts8.x1.international.travian.com--P--5576", player_server_id: 5576, player_name: "kprincess", alliance_id: "https://ts8.x1.international.travian.com--A--299", alliance_server_id: 299, alliance_name: "Braz", population: 269, region: nil, is_capital: false, is_city: nil, victory_points: nil},
  
]
agg_players = [
  %Collector.AggPlayers{target_dt: ~U[2023-04-14 00:00:00.000Z], server_id: "https://ts8.x1.international.travian.com", player_id: "https://ts8.x1.international.travian.com--P--2051", estimated_starting_date: ~D[2023-04-15], estimated_tribe: 3, increment: [%Collector.AggPlayers.Increment{target_dt: ~U[2023-04-14 00:00:00.000Z], total_population: 1047, population_increase: nil, population_increase_by_founded: nil, population_increase_by_conquered: nil, population_decrease: nil, population_decrease_by_conquered: nil, population_decrease_by_destroyed: nil, total_villages: 3, n_villages_with_population_increase: nil, n_villages_with_population_decrease: nil, n_villages_with_population_stuck: nil, new_village_founded: nil, new_village_conquered: nil, lost_village_conquered: nil, lost_village_destroyed: nil}]},
  %Collector.AggPlayers{target_dt: ~U[2023-04-15 00:00:00.000Z], server_id: "https://ts8.x1.international.travian.com", player_id: "https://ts8.x1.international.travian.com--P--5576", estimated_starting_date: ~D[2023-04-15], estimated_tribe: 3, increment: [%Collector.AggPlayers.Increment{target_dt: ~U[2023-04-15 00:00:00.000Z], total_population: 1032, population_increase: nil, population_increase_by_founded: nil, population_increase_by_conquered: nil, population_decrease: nil, population_decrease_by_conquered: nil, population_decrease_by_destroyed: nil, total_villages: 3, n_villages_with_population_increase: nil, n_villages_with_population_decrease: nil, n_villages_with_population_stuck: nil, new_village_founded: nil, new_village_conquered: nil, lost_village_conquered: nil, lost_village_destroyed: nil}]}
]

agg_server = %Collector.AggServer{target_dt: ~U[2023-04-15 00:00:00.000Z], server_id: "https://ts8.x1.international.travian.com", url: "https://ts8.x1.international.travian.com", shrink: "ts8", speed: 1, region: "international", estimated_starting_date: ~D[2023-04-14], increment: [%Collector.AggServer.Increment{target_dt: ~U[2023-04-15 00:00:00.000Z], natar_villages: 503, natar_population: 66449, natar_population_variation: nil, total_population: 1890046, population_variation: nil, total_villages: 7226, new_villages: nil, removed_villages: nil, total_players: 4576, new_players: nil, removed_players: nil, total_alliances: 163, new_alliances: nil, removed_alliances: nil}]}

    :ok = Collector.Feed.store(root_folder, server_id, target_date, snapshot, Collector.Snapshot)
    :ok = Collector.Feed.store(root_folder, server_id, target_date, agg_players, Collector.AggPlayers)
    :ok = Collector.Feed.store(root_folder, server_id, target_date, agg_server, Collector.AggServer)

    :ok = Collector.MedusaPredInput.run(root_folder, server_id, target_date)
    {:ok, medusa_pred_input} = Collector.Feed.open(root_folder, server_id, target_date, Collector.MedusaPredInput)

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

  test "process fills the nil values with 0 when there is only 1 increment" do
  end

  test "process fills the N sample when there is an N increment avilable, otherwise, default value with :t_N_has_data? as 0" do
  end

  test "if there is a previous increment available, the :t_N_has_increase? flag should be 1" do
  end


  test "if there are more than 7 samples, only use the last 7" do
  end
end
