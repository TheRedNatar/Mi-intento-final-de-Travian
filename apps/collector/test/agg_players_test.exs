defmodule Collector.AggPlayersTest do
  use ExUnit.Case







  test "AggPlayers.process() generate a new increment for no new players or create an init for the new ones" do
    target_dt = DateTime.utc_now()
    new_target_dt = DateTime.add(target_dt, 3600*24)
    server_id = "server1"

new_player_snapshot = [
      %Collector.SnapshotRow{grid_position: 20, x: -181, y: 200, tribe: 2, village_id: "v1", village_server_id: 19995, village_name: "n1", player_id: "p1", player_server_id: 361, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 961, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 49, x: -152, y: 200, tribe: 1, village_id: "p2", village_server_id: 19702, village_name: "a2", player_id: "p2", player_server_id: 416, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 964, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 30, x: -151, y: 180, tribe: 3, village_id: "p3", village_server_id: 19996, village_name: "a3", player_id: "p2", player_server_id: 361, player_name: "laskdj", alliance_id: "a1", alliance_server_id: 8, alliance_name: "alskj", population: 1200, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 30, x: -151, y: 180, tribe: 4, village_id: "p4", village_server_id: 19996, village_name: "a3", player_id: "p3", player_server_id: 361, player_name: "laskdj", alliance_id: "a1", alliance_server_id: 8, alliance_name: "alskj", population: 1200, region: nil, is_capital: false, is_city: nil, victory_points: nil}
    ]
prev_player_snapshot = [
      %Collector.SnapshotRow{grid_position: 20, x: -181, y: 200, tribe: 2, village_id: "v1", village_server_id: 19995, village_name: "n1", player_id: "p1", player_server_id: 361, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 961, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 49, x: -152, y: 200, tribe: 2, village_id: "p2", village_server_id: 19702, village_name: "a2", player_id: "p1", player_server_id: 416, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 964, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 30, x: -151, y: 180, tribe: 3, village_id: "p3", village_server_id: 19996, village_name: "a3", player_id: "p2", player_server_id: 361, player_name: "laskdj", alliance_id: "a2", alliance_server_id: 8, alliance_name: "alskj", population: 1200, region: nil, is_capital: false, is_city: nil, victory_points: nil}]


      init_agg_players = Collector.AggPlayers.process(target_dt, server_id, prev_player_snapshot)
      new_agg_players = [p1, p2, p3] = Collector.AggPlayers.process(new_target_dt, server_id, new_player_snapshot, prev_player_snapshot, init_agg_players) |> Enum.sort_by(&(&1.player_id))


      for agg_player <- new_agg_players, do: assert(is_struct(agg_player, Collector.AggPlayers))


      assert(p1.target_dt == new_target_dt)
      assert(p1.server_id == server_id)
      assert(p1.estimated_starting_date == DateTime.to_date(target_dt))
      assert(p1.estimated_tribe == 2)
      last_inc_p1 = hd(p1.increment)
      assert(last_inc_p1.lost_village_conquered == 1)

      assert(p2.target_dt == new_target_dt)
      assert(p2.server_id == server_id)
      assert(p2.estimated_starting_date == DateTime.to_date(target_dt))
      assert(p2.estimated_tribe == 3)
      last_inc_p2 = hd(p2.increment)
      assert(last_inc_p2.population_increase_by_conquered == 964)

      assert(p3.target_dt == new_target_dt)
      assert(p3.server_id == server_id)
      assert(p3.estimated_starting_date == DateTime.to_date(new_target_dt))
      assert(p3.estimated_tribe == 4)
  end


  test "AggPlayers.process() while init defines de estimated_tribe and estimated starting_date for all the players" do
    target_dt = DateTime.utc_now()
    player_id = "p1"
    server_id = "server1"

new_player_snapshot = [
      %Collector.SnapshotRow{grid_position: 20, x: -181, y: 200, tribe: 2, village_id: "v1", village_server_id: 19995, village_name: "n1", player_id: "p1", player_server_id: 361, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 961, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 49, x: -152, y: 200, tribe: 2, village_id: "p2", village_server_id: 19702, village_name: "a2", player_id: "p1", player_server_id: 416, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 964, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 30, x: -151, y: 180, tribe: 3, village_id: "p3", village_server_id: 19996, village_name: "a3", player_id: "p2", player_server_id: 361, player_name: "laskdj", alliance_id: "a2", alliance_server_id: 8, alliance_name: "alskj", population: 1200, region: nil, is_capital: false, is_city: nil, victory_points: nil}]

      init_agg_players = [p1, p2] = Collector.AggPlayers.process(target_dt, server_id, new_player_snapshot) |> Enum.sort_by(&(&1.player_id))

      for agg_player <- init_agg_players, do: assert(is_struct(agg_player, Collector.AggPlayers))

      assert(p1.target_dt == target_dt)
      assert(p1.server_id == server_id)
      assert(p1.estimated_starting_date == DateTime.to_date(target_dt))
      assert(p1.estimated_tribe == 2)

      assert(p2.target_dt == target_dt)
      assert(p2.server_id == server_id)
      assert(p2.estimated_starting_date == DateTime.to_date(target_dt))
      assert(p2.estimated_tribe == 3)
  end


  test "AggPlayers.increment() defines a village as conquered if it is new for the player and it was owned by another player in the previous snapshot and the value of new_village_conquered is the count of these villages" do
    target_dt = DateTime.utc_now()
    player_id = "p1"

new_player_snapshot = [
      %Collector.SnapshotRow{grid_position: 20, x: -181, y: 200, tribe: 2, village_id: "v1", village_server_id: 19995, village_name: "n1", player_id: "p1", player_server_id: 361, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 961, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 49, x: -152, y: 200, tribe: 1, village_id: "p2", village_server_id: 19702, village_name: "a2", player_id: "p1", player_server_id: 416, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 964, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 30, x: -151, y: 180, tribe: 3, village_id: "p3", village_server_id: 19996, village_name: "a3", player_id: "p1", player_server_id: 361, player_name: "laskdj", alliance_id: "a1", alliance_server_id: 8, alliance_name: "alskj", population: 1200, region: nil, is_capital: false, is_city: nil, victory_points: nil}
    ]
prev_player_snapshot = [
      %Collector.SnapshotRow{grid_position: 20, x: -181, y: 200, tribe: 2, village_id: "v1", village_server_id: 19995, village_name: "n1", player_id: "p1", player_server_id: 361, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 961, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 49, x: -152, y: 200, tribe: 1, village_id: "p2", village_server_id: 19702, village_name: "a2", player_id: "p1", player_server_id: 416, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 964, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 30, x: -151, y: 180, tribe: 3, village_id: "p3", village_server_id: 19996, village_name: "a3", player_id: "p2", player_server_id: 361, player_name: "laskdj", alliance_id: "a2", alliance_server_id: 8, alliance_name: "alskj", population: 1200, region: nil, is_capital: false, is_city: nil, victory_points: nil}]

      inc = Collector.AggPlayers.increment(target_dt, player_id, new_player_snapshot, prev_player_snapshot)
      assert(is_struct(inc, Collector.AggPlayers.Increment))
      assert(inc.target_dt == target_dt)
      assert(inc.new_village_conquered == 1)
  end

  test "AggPlayers.increment() defines a village as founded if it is new for the player was and it wasn't owned by another player in the previous snapshot" do
    target_dt = DateTime.utc_now()
    player_id = "p1"

new_player_snapshot = [
      %Collector.SnapshotRow{grid_position: 20, x: -181, y: 200, tribe: 2, village_id: "v1", village_server_id: 19995, village_name: "n1", player_id: "p1", player_server_id: 361, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 961, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 49, x: -152, y: 200, tribe: 1, village_id: "p2", village_server_id: 19702, village_name: "a2", player_id: "p1", player_server_id: 416, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 964, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 30, x: -151, y: 180, tribe: 3, village_id: "p3", village_server_id: 19996, village_name: "a3", player_id: "p1", player_server_id: 361, player_name: "laskdj", alliance_id: "a1", alliance_server_id: 8, alliance_name: "alskj", population: 1200, region: nil, is_capital: false, is_city: nil, victory_points: nil}
    ]
prev_player_snapshot = [
      %Collector.SnapshotRow{grid_position: 20, x: -181, y: 200, tribe: 2, village_id: "v1", village_server_id: 19995, village_name: "n1", player_id: "p1", player_server_id: 361, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 961, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 49, x: -152, y: 200, tribe: 1, village_id: "p2", village_server_id: 19702, village_name: "a2", player_id: "p1", player_server_id: 416, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 964, region: nil, is_capital: false, is_city: nil, victory_points: nil}]

      inc = Collector.AggPlayers.increment(target_dt, player_id, new_player_snapshot, prev_player_snapshot)
      assert(is_struct(inc, Collector.AggPlayers.Increment))
      assert(inc.target_dt == target_dt)
      assert(inc.new_village_founded == 1)
  end

  test "AggPlayers.increment() defines a village as lost_destroyed if it doesn't appear in the new snapshot and it was owned by the player in the previous snapshot" do
    target_dt = DateTime.utc_now()
    player_id = "p1"

new_player_snapshot = [
      %Collector.SnapshotRow{grid_position: 20, x: -181, y: 200, tribe: 2, village_id: "v1", village_server_id: 19995, village_name: "n1", player_id: "p1", player_server_id: 361, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 961, region: nil, is_capital: false, is_city: nil, victory_points: nil}
    ]
prev_player_snapshot = [
      %Collector.SnapshotRow{grid_position: 20, x: -181, y: 200, tribe: 2, village_id: "v1", village_server_id: 19995, village_name: "n1", player_id: "p1", player_server_id: 361, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 961, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 49, x: -152, y: 200, tribe: 1, village_id: "p2", village_server_id: 19702, village_name: "a2", player_id: "p1", player_server_id: 416, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 964, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 30, x: -151, y: 180, tribe: 3, village_id: "p3", village_server_id: 19996, village_name: "a3", player_id: "p1", player_server_id: 361, player_name: "laskdj", alliance_id: "a2", alliance_server_id: 8, alliance_name: "alskj", population: 1200, region: nil, is_capital: false, is_city: nil, victory_points: nil}]

      inc = Collector.AggPlayers.increment(target_dt, player_id, new_player_snapshot, prev_player_snapshot)
      assert(is_struct(inc, Collector.AggPlayers.Increment))
      assert(inc.target_dt == target_dt)
      assert(inc.lost_village_destroyed == 2)
  end

  test "AggPlayers.increment() defines a village as lost_conquered if it is owned by another player in the new snapshot" do

    target_dt = DateTime.utc_now()
    player_id = "p1"

new_player_snapshot = [
      %Collector.SnapshotRow{grid_position: 49, x: -152, y: 200, tribe: 1, village_id: "p2", village_server_id: 19702, village_name: "a2", player_id: "p2", player_server_id: 416, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 964, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 30, x: -151, y: 180, tribe: 3, village_id: "p3", village_server_id: 19996, village_name: "a3", player_id: "p1", player_server_id: 361, player_name: "laskdj", alliance_id: "a1", alliance_server_id: 8, alliance_name: "alskj", population: 1200, region: nil, is_capital: false, is_city: nil, victory_points: nil}
    ]
prev_player_snapshot = [
      %Collector.SnapshotRow{grid_position: 20, x: -181, y: 200, tribe: 2, village_id: "v1", village_server_id: 19995, village_name: "n1", player_id: "p1", player_server_id: 361, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 961, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 49, x: -152, y: 200, tribe: 1, village_id: "p2", village_server_id: 19702, village_name: "a2", player_id: "p1", player_server_id: 416, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 964, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 30, x: -151, y: 180, tribe: 3, village_id: "p3", village_server_id: 19996, village_name: "a3", player_id: "p1", player_server_id: 361, player_name: "laskdj", alliance_id: "a2", alliance_server_id: 8, alliance_name: "alskj", population: 1200, region: nil, is_capital: false, is_city: nil, victory_points: nil}]

      inc = Collector.AggPlayers.increment(target_dt, player_id, new_player_snapshot, prev_player_snapshot)
      assert(is_struct(inc, Collector.AggPlayers.Increment))
      assert(inc.target_dt == target_dt)
      assert(inc.lost_village_conquered == 1)
  end

  test "AggPlayers.increment() defines total_villages as the count of the villages in the new snapshot " do
    target_dt = DateTime.utc_now()
    player_id = "p1"

new_player_snapshot = [
      %Collector.SnapshotRow{grid_position: 20, x: -181, y: 200, tribe: 2, village_id: "v1", village_server_id: 19995, village_name: "n1", player_id: "p1", player_server_id: 361, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 961, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 49, x: -152, y: 200, tribe: 1, village_id: "p2", village_server_id: 19702, village_name: "a2", player_id: "p1", player_server_id: 416, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 964, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 30, x: -151, y: 180, tribe: 3, village_id: "p3", village_server_id: 19996, village_name: "a3", player_id: "p1", player_server_id: 361, player_name: "laskdj", alliance_id: "a1", alliance_server_id: 8, alliance_name: "alskj", population: 1200, region: nil, is_capital: false, is_city: nil, victory_points: nil}
    ]
prev_player_snapshot = [
      %Collector.SnapshotRow{grid_position: 20, x: -181, y: 200, tribe: 2, village_id: "v1", village_server_id: 19995, village_name: "n1", player_id: "p1", player_server_id: 361, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 961, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 30, x: -151, y: 180, tribe: 3, village_id: "p3", village_server_id: 19996, village_name: "a3", player_id: "p2", player_server_id: 361, player_name: "laskdj", alliance_id: "a2", alliance_server_id: 8, alliance_name: "alskj", population: 1200, region: nil, is_capital: false, is_city: nil, victory_points: nil}]

      inc = Collector.AggPlayers.increment(target_dt, player_id, new_player_snapshot, prev_player_snapshot)
      assert(is_struct(inc, Collector.AggPlayers.Increment))
      assert(inc.target_dt == target_dt)
      assert(inc.total_villages == 3)
  end

  test "AggPlayers.increment() defines total_population as the sum of village's populations in the new snapshot" do
    target_dt = DateTime.utc_now()
    player_id = "p1"

new_player_snapshot = [
      %Collector.SnapshotRow{grid_position: 20, x: -181, y: 200, tribe: 2, village_id: "v1", village_server_id: 19995, village_name: "n1", player_id: "p1", player_server_id: 361, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 961, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 30, x: -151, y: 180, tribe: 3, village_id: "p3", village_server_id: 19996, village_name: "a3", player_id: "p1", player_server_id: 361, player_name: "laskdj", alliance_id: "a1", alliance_server_id: 8, alliance_name: "alskj", population: 1200, region: nil, is_capital: false, is_city: nil, victory_points: nil}
    ]
prev_player_snapshot = [
      %Collector.SnapshotRow{grid_position: 20, x: -181, y: 200, tribe: 2, village_id: "v1", village_server_id: 19995, village_name: "n1", player_id: "p1", player_server_id: 361, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 961, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 49, x: -152, y: 200, tribe: 1, village_id: "p2", village_server_id: 19702, village_name: "a2", player_id: "p1", player_server_id: 416, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 964, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 30, x: -151, y: 180, tribe: 3, village_id: "p3", village_server_id: 19996, village_name: "a3", player_id: "p2", player_server_id: 361, player_name: "laskdj", alliance_id: "a2", alliance_server_id: 8, alliance_name: "alskj", population: 1200, region: nil, is_capital: false, is_city: nil, victory_points: nil}]

      inc = Collector.AggPlayers.increment(target_dt, player_id, new_player_snapshot, prev_player_snapshot)
      assert(is_struct(inc, Collector.AggPlayers.Increment))
      assert(inc.target_dt == target_dt)
      assert(inc.total_population == 2161)
  end

  test "AggPlayers.increment() defines population_increase as the sum of village's populations increment if this increment is positive" do
    target_dt = DateTime.utc_now()
    player_id = "p1"


new_player_snapshot = [
      %Collector.SnapshotRow{grid_position: 20, x: -181, y: 200, tribe: 2, village_id: "v1", village_server_id: 19995, village_name: "n1", player_id: "p1", player_server_id: 361, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 900, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 49, x: -152, y: 200, tribe: 1, village_id: "p2", village_server_id: 19702, village_name: "a2", player_id: "p1", player_server_id: 416, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 1000, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 30, x: -151, y: 180, tribe: 3, village_id: "p3", village_server_id: 19996, village_name: "a3", player_id: "p1", player_server_id: 361, player_name: "laskdj", alliance_id: "a1", alliance_server_id: 8, alliance_name: "alskj", population: 1200, region: nil, is_capital: false, is_city: nil, victory_points: nil}
    ]
prev_player_snapshot = [
      %Collector.SnapshotRow{grid_position: 20, x: -181, y: 200, tribe: 2, village_id: "v1", village_server_id: 19995, village_name: "n1", player_id: "p1", player_server_id: 361, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 961, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 49, x: -152, y: 200, tribe: 1, village_id: "p2", village_server_id: 19702, village_name: "a2", player_id: "p1", player_server_id: 416, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 964, region: nil, is_capital: false, is_city: nil, victory_points: nil}]

      inc = Collector.AggPlayers.increment(target_dt, player_id, new_player_snapshot, prev_player_snapshot)
      assert(is_struct(inc, Collector.AggPlayers.Increment))
      assert(inc.target_dt == target_dt)
      assert(inc.population_increase == 36)
  end

  test "AggPlayers.increment() defines population_increase_by_founded as the sum of founded village's populations in new snapshot" do
    target_dt = DateTime.utc_now()
    player_id = "p1"


new_player_snapshot = [
      %Collector.SnapshotRow{grid_position: 20, x: -181, y: 200, tribe: 2, village_id: "v1", village_server_id: 19995, village_name: "n1", player_id: "p1", player_server_id: 361, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 961, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 49, x: -152, y: 200, tribe: 1, village_id: "p2", village_server_id: 19702, village_name: "a2", player_id: "p1", player_server_id: 416, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 964, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 30, x: -151, y: 180, tribe: 3, village_id: "p3", village_server_id: 19996, village_name: "a3", player_id: "p1", player_server_id: 361, player_name: "laskdj", alliance_id: "a1", alliance_server_id: 8, alliance_name: "alskj", population: 1200, region: nil, is_capital: false, is_city: nil, victory_points: nil}
    ]
prev_player_snapshot = [
      %Collector.SnapshotRow{grid_position: 20, x: -181, y: 200, tribe: 2, village_id: "v1", village_server_id: 19995, village_name: "n1", player_id: "p1", player_server_id: 361, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 961, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 49, x: -152, y: 200, tribe: 1, village_id: "p2", village_server_id: 19702, village_name: "a2", player_id: "p1", player_server_id: 416, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 964, region: nil, is_capital: false, is_city: nil, victory_points: nil}]

      inc = Collector.AggPlayers.increment(target_dt, player_id, new_player_snapshot, prev_player_snapshot)
      assert(is_struct(inc, Collector.AggPlayers.Increment))
      assert(inc.target_dt == target_dt)
      assert(inc.population_increase_by_founded == 1200)
  end

  test "AggPlayers.increment() defines population_increase_by_conquered as the sum of conquered village's populations in new snapshot" do
    target_dt = DateTime.utc_now()
    player_id = "p1"

new_player_snapshot = [
      %Collector.SnapshotRow{grid_position: 20, x: -181, y: 200, tribe: 2, village_id: "v1", village_server_id: 19995, village_name: "n1", player_id: "p1", player_server_id: 361, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 961, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 49, x: -152, y: 200, tribe: 1, village_id: "p2", village_server_id: 19702, village_name: "a2", player_id: "p1", player_server_id: 416, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 964, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 30, x: -151, y: 180, tribe: 3, village_id: "p3", village_server_id: 19996, village_name: "a3", player_id: "p1", player_server_id: 361, player_name: "laskdj", alliance_id: "a1", alliance_server_id: 8, alliance_name: "alskj", population: 1200, region: nil, is_capital: false, is_city: nil, victory_points: nil}
    ]
prev_player_snapshot = [
      %Collector.SnapshotRow{grid_position: 20, x: -181, y: 200, tribe: 2, village_id: "v1", village_server_id: 19995, village_name: "n1", player_id: "p1", player_server_id: 361, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 961, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 49, x: -152, y: 200, tribe: 1, village_id: "p2", village_server_id: 19702, village_name: "a2", player_id: "p2", player_server_id: 416, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 964, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 30, x: -151, y: 180, tribe: 3, village_id: "p3", village_server_id: 19996, village_name: "a3", player_id: "p2", player_server_id: 361, player_name: "laskdj", alliance_id: "a2", alliance_server_id: 8, alliance_name: "alskj", population: 1200, region: nil, is_capital: false, is_city: nil, victory_points: nil}]

      inc = Collector.AggPlayers.increment(target_dt, player_id, new_player_snapshot, prev_player_snapshot)
      assert(is_struct(inc, Collector.AggPlayers.Increment))
      assert(inc.target_dt == target_dt)
      assert(inc.population_increase_by_conquered == 2164)
  end

  test "AggPlayers.increment() defines population_decrease as the sum of village's populations increment if this increment is negative, it is an absolute value" do
    target_dt = DateTime.utc_now()
    player_id = "p1"

new_player_snapshot = [
      %Collector.SnapshotRow{grid_position: 20, x: -181, y: 200, tribe: 2, village_id: "v1", village_server_id: 19995, village_name: "n1", player_id: "p1", player_server_id: 361, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 900, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 49, x: -152, y: 200, tribe: 1, village_id: "p2", village_server_id: 19702, village_name: "a2", player_id: "p1", player_server_id: 416, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 964, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 30, x: -151, y: 180, tribe: 3, village_id: "p3", village_server_id: 19996, village_name: "a3", player_id: "p1", player_server_id: 361, player_name: "laskdj", alliance_id: "a1", alliance_server_id: 8, alliance_name: "alskj", population: 1200, region: nil, is_capital: false, is_city: nil, victory_points: nil}
    ]
prev_player_snapshot = [
      %Collector.SnapshotRow{grid_position: 20, x: -181, y: 200, tribe: 2, village_id: "v1", village_server_id: 19995, village_name: "n1", player_id: "p1", player_server_id: 361, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 961, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 49, x: -152, y: 200, tribe: 1, village_id: "p2", village_server_id: 19702, village_name: "a2", player_id: "p1", player_server_id: 416, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 964, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 30, x: -151, y: 180, tribe: 3, village_id: "p3", village_server_id: 19996, village_name: "a3", player_id: "p2", player_server_id: 361, player_name: "laskdj", alliance_id: "a2", alliance_server_id: 8, alliance_name: "alskj", population: 1200, region: nil, is_capital: false, is_city: nil, victory_points: nil}]

      inc = Collector.AggPlayers.increment(target_dt, player_id, new_player_snapshot, prev_player_snapshot)
      assert(is_struct(inc, Collector.AggPlayers.Increment))
      assert(inc.target_dt == target_dt)
      assert(inc.population_decrease == 61)
  end

  test "AggPlayers.increment() defines population_decrease_by_conquered as the sum of lost_conquered village's populations in new snapshot" do
    target_dt = DateTime.utc_now()
    player_id = "p1"

new_player_snapshot = [
      %Collector.SnapshotRow{grid_position: 20, x: -181, y: 200, tribe: 2, village_id: "v1", village_server_id: 19995, village_name: "n1", player_id: "p1", player_server_id: 361, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 961, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 49, x: -152, y: 200, tribe: 1, village_id: "p2", village_server_id: 19702, village_name: "a2", player_id: "p3", player_server_id: 416, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 964, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 30, x: -151, y: 180, tribe: 3, village_id: "p3", village_server_id: 19996, village_name: "a3", player_id: "p1", player_server_id: 361, player_name: "laskdj", alliance_id: "a1", alliance_server_id: 8, alliance_name: "alskj", population: 1200, region: nil, is_capital: false, is_city: nil, victory_points: nil}
    ]
prev_player_snapshot = [
      %Collector.SnapshotRow{grid_position: 20, x: -181, y: 200, tribe: 2, village_id: "v1", village_server_id: 19995, village_name: "n1", player_id: "p1", player_server_id: 361, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 961, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 49, x: -152, y: 200, tribe: 1, village_id: "p2", village_server_id: 19702, village_name: "a2", player_id: "p1", player_server_id: 416, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 964, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 30, x: -151, y: 180, tribe: 3, village_id: "p3", village_server_id: 19996, village_name: "a3", player_id: "p2", player_server_id: 361, player_name: "laskdj", alliance_id: "a2", alliance_server_id: 8, alliance_name: "alskj", population: 1200, region: nil, is_capital: false, is_city: nil, victory_points: nil}]

      inc = Collector.AggPlayers.increment(target_dt, player_id, new_player_snapshot, prev_player_snapshot)
      assert(is_struct(inc, Collector.AggPlayers.Increment))
      assert(inc.target_dt == target_dt)
      assert(inc.population_decrease_by_conquered == 964)
  end

  test "AggPlayers.increment() defines population_decrease_by_destroyed as the sum of destroyed village's populations in previous snapshot" do
    target_dt = DateTime.utc_now()
    player_id = "p1"

new_player_snapshot = [
      %Collector.SnapshotRow{grid_position: 20, x: -181, y: 200, tribe: 2, village_id: "v1", village_server_id: 19995, village_name: "n1", player_id: "p1", player_server_id: 361, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 961, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 30, x: -151, y: 180, tribe: 3, village_id: "p3", village_server_id: 19996, village_name: "a3", player_id: "p1", player_server_id: 361, player_name: "laskdj", alliance_id: "a1", alliance_server_id: 8, alliance_name: "alskj", population: 1200, region: nil, is_capital: false, is_city: nil, victory_points: nil}
    ]
prev_player_snapshot = [
      %Collector.SnapshotRow{grid_position: 20, x: -181, y: 200, tribe: 2, village_id: "v1", village_server_id: 19995, village_name: "n1", player_id: "p1", player_server_id: 361, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 961, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 49, x: -152, y: 200, tribe: 1, village_id: "p2", village_server_id: 19702, village_name: "a2", player_id: "p1", player_server_id: 416, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 950, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 30, x: -151, y: 180, tribe: 3, village_id: "p3", village_server_id: 19996, village_name: "a3", player_id: "p2", player_server_id: 361, player_name: "laskdj", alliance_id: "a2", alliance_server_id: 8, alliance_name: "alskj", population: 1200, region: nil, is_capital: false, is_city: nil, victory_points: nil}]

      inc = Collector.AggPlayers.increment(target_dt, player_id, new_player_snapshot, prev_player_snapshot)
      assert(is_struct(inc, Collector.AggPlayers.Increment))
      assert(inc.target_dt == target_dt)
      assert(inc.population_decrease_by_destroyed == 950)
  end

  test "AggPlayers.increment() defines n_villages_with_population_increase as the count of villages whos population increment is positive" do
    target_dt = DateTime.utc_now()
    player_id = "p1"

new_player_snapshot = [
      %Collector.SnapshotRow{grid_position: 20, x: -181, y: 200, tribe: 2, village_id: "v1", village_server_id: 19995, village_name: "n1", player_id: "p1", player_server_id: 361, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 1000, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 49, x: -152, y: 200, tribe: 1, village_id: "p2", village_server_id: 19702, village_name: "a2", player_id: "p1", player_server_id: 416, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 1000, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 30, x: -151, y: 180, tribe: 3, village_id: "p3", village_server_id: 19996, village_name: "a3", player_id: "p1", player_server_id: 361, player_name: "laskdj", alliance_id: "a1", alliance_server_id: 8, alliance_name: "alskj", population: 1200, region: nil, is_capital: false, is_city: nil, victory_points: nil}
    ]
prev_player_snapshot = [
      %Collector.SnapshotRow{grid_position: 20, x: -181, y: 200, tribe: 2, village_id: "v1", village_server_id: 19995, village_name: "n1", player_id: "p1", player_server_id: 361, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 961, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 49, x: -152, y: 200, tribe: 1, village_id: "p2", village_server_id: 19702, village_name: "a2", player_id: "p1", player_server_id: 416, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 964, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 30, x: -151, y: 180, tribe: 3, village_id: "p3", village_server_id: 19996, village_name: "a3", player_id: "p2", player_server_id: 361, player_name: "laskdj", alliance_id: "a2", alliance_server_id: 8, alliance_name: "alskj", population: 1200, region: nil, is_capital: false, is_city: nil, victory_points: nil}]

      inc = Collector.AggPlayers.increment(target_dt, player_id, new_player_snapshot, prev_player_snapshot)
      assert(is_struct(inc, Collector.AggPlayers.Increment))
      assert(inc.target_dt == target_dt)
      assert(inc.n_villages_with_population_increase == 2)
  end

  test "AggPlayers.increment() defines n_villages_with_population_decrease as the count of villages whos population increment is negative" do
    target_dt = DateTime.utc_now()
    player_id = "p1"

new_player_snapshot = [
      %Collector.SnapshotRow{grid_position: 20, x: -181, y: 200, tribe: 2, village_id: "v1", village_server_id: 19995, village_name: "n1", player_id: "p1", player_server_id: 361, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 100, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 49, x: -152, y: 200, tribe: 1, village_id: "p2", village_server_id: 19702, village_name: "a2", player_id: "p1", player_server_id: 416, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 1000, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 30, x: -151, y: 180, tribe: 3, village_id: "p3", village_server_id: 19996, village_name: "a3", player_id: "p1", player_server_id: 361, player_name: "laskdj", alliance_id: "a1", alliance_server_id: 8, alliance_name: "alskj", population: 1200, region: nil, is_capital: false, is_city: nil, victory_points: nil}
    ]
prev_player_snapshot = [
      %Collector.SnapshotRow{grid_position: 20, x: -181, y: 200, tribe: 2, village_id: "v1", village_server_id: 19995, village_name: "n1", player_id: "p1", player_server_id: 361, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 961, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 49, x: -152, y: 200, tribe: 1, village_id: "p2", village_server_id: 19702, village_name: "a2", player_id: "p1", player_server_id: 416, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 964, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 30, x: -151, y: 180, tribe: 3, village_id: "p3", village_server_id: 19996, village_name: "a3", player_id: "p2", player_server_id: 361, player_name: "laskdj", alliance_id: "a2", alliance_server_id: 8, alliance_name: "alskj", population: 1200, region: nil, is_capital: false, is_city: nil, victory_points: nil}]

      inc = Collector.AggPlayers.increment(target_dt, player_id, new_player_snapshot, prev_player_snapshot)
      assert(is_struct(inc, Collector.AggPlayers.Increment))
      assert(inc.target_dt == target_dt)
      assert(inc.n_villages_with_population_decrease == 1)
  end

  test "AggPlayers.increment() defines n_villages_with_population_stuck as the count of villages whos population increment is 0" do
    target_dt = DateTime.utc_now()
    player_id = "p1"

new_player_snapshot = [
      %Collector.SnapshotRow{grid_position: 20, x: -181, y: 200, tribe: 2, village_id: "v1", village_server_id: 19995, village_name: "n1", player_id: "p1", player_server_id: 361, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 961, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 49, x: -152, y: 200, tribe: 1, village_id: "p2", village_server_id: 19702, village_name: "a2", player_id: "p1", player_server_id: 416, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 964, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 30, x: -151, y: 180, tribe: 3, village_id: "p3", village_server_id: 19996, village_name: "a3", player_id: "p1", player_server_id: 361, player_name: "laskdj", alliance_id: "a1", alliance_server_id: 8, alliance_name: "alskj", population: 1200, region: nil, is_capital: false, is_city: nil, victory_points: nil}
    ]
prev_player_snapshot = [
      %Collector.SnapshotRow{grid_position: 20, x: -181, y: 200, tribe: 2, village_id: "v1", village_server_id: 19995, village_name: "n1", player_id: "p1", player_server_id: 361, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 961, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 49, x: -152, y: 200, tribe: 1, village_id: "p2", village_server_id: 19702, village_name: "a2", player_id: "p1", player_server_id: 416, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 964, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 30, x: -151, y: 180, tribe: 3, village_id: "p3", village_server_id: 19996, village_name: "a3", player_id: "p2", player_server_id: 361, player_name: "laskdj", alliance_id: "a2", alliance_server_id: 8, alliance_name: "alskj", population: 1200, region: nil, is_capital: false, is_city: nil, victory_points: nil}]

      inc = Collector.AggPlayers.increment(target_dt, player_id, new_player_snapshot, prev_player_snapshot)
      assert(is_struct(inc, Collector.AggPlayers.Increment))
      assert(inc.target_dt == target_dt)
      assert(inc.n_villages_with_population_stuck == 2)
  end

  test "If there is no previous snapshot, AggPlayers.increment() defines increment fields as nil" do
    target_dt = DateTime.utc_now()

new_player_snapshot = [
      %Collector.SnapshotRow{grid_position: 20, x: -181, y: 200, tribe: 2, village_id: "v1", village_server_id: 19995, village_name: "n1", player_id: "p1", player_server_id: 361, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 961, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 49, x: -152, y: 200, tribe: 1, village_id: "p2", village_server_id: 19702, village_name: "a2", player_id: "p1", player_server_id: 416, player_name: "opc", alliance_id: "a1", alliance_server_id: 8, alliance_name: "WW", population: 964, region: nil, is_capital: false, is_city: nil, victory_points: nil},
      %Collector.SnapshotRow{grid_position: 30, x: -151, y: 180, tribe: 3, village_id: "p3", village_server_id: 19996, village_name: "a3", player_id: "p1", player_server_id: 361, player_name: "laskdj", alliance_id: "a1", alliance_server_id: 8, alliance_name: "alskj", population: 1200, region: nil, is_capital: false, is_city: nil, victory_points: nil}
    ]

expected_output = 
    %Collector.AggPlayers.Increment{
      target_dt: target_dt,
      total_population: 3125,
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

      inc = Collector.AggPlayers.increment(target_dt, new_player_snapshot)
      assert(expected_output == inc)
  end


end
