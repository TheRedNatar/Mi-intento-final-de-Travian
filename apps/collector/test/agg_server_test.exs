defmodule Collector.AggServerTest do
  use ExUnit.Case

  @tag :tmp_dir
  test "AggServer.run() returns unable to open the file if there is no snapshot of target_date",
       %{tmp_dir: root_folder} do
    target_date = Date.utc_today()
    server_id = "server1"

    {atom_error, {string_agg_open, _}} =
      Collector.AggServer.run(root_folder, server_id, target_date)

    assert(atom_error == :error)
    assert(string_agg_open == "Unable to open target_date snapshot")
  end

  @tag :tmp_dir
  test "AggServer.run() if there is previous snapshot but not previous agg_server returns error",
       %{tmp_dir: root_folder} do
    target_date = Date.utc_today()
    prev_date = Date.add(target_date, -1)
    server_id = "server1"

    new_snapshot = [
      %Collector.Snapshot{
        map_id: 20,
        x: -181,
        y: 200,
        tribe: 2,
        village_id: "v1",
        village_server_id: 19995,
        village_name: "n1",
        player_id: "p1",
        player_server_id: 361,
        player_name: "opc",
        alliance_id: "a1",
        alliance_server_id: 8,
        alliance_name: "WW",
        population: 961,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      },
      %Collector.Snapshot{
        map_id: 49,
        x: -152,
        y: 200,
        tribe: 1,
        village_id: "p2",
        village_server_id: 19702,
        village_name: "a2",
        player_id: "p2",
        player_server_id: 416,
        player_name: "opc",
        alliance_id: "a1",
        alliance_server_id: 8,
        alliance_name: "WW",
        population: 964,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      },
      %Collector.Snapshot{
        map_id: 30,
        x: -151,
        y: 180,
        tribe: 3,
        village_id: "p3",
        village_server_id: 19996,
        village_name: "a3",
        player_id: "p2",
        player_server_id: 361,
        player_name: "laskdj",
        alliance_id: "a1",
        alliance_server_id: 8,
        alliance_name: "alskj",
        population: 1200,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      },
      %Collector.Snapshot{
        map_id: 30,
        x: -151,
        y: 180,
        tribe: 4,
        village_id: "p4",
        village_server_id: 19996,
        village_name: "a3",
        player_id: "p3",
        player_server_id: 361,
        player_name: "laskdj",
        alliance_id: "a1",
        alliance_server_id: 8,
        alliance_name: "alskj",
        population: 1200,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      }
    ]

    assert(
      :ok ==
        Collector.Feed.store(root_folder, server_id, prev_date, new_snapshot, Collector.Snapshot)
    )

    assert(
      :ok ==
        Collector.Feed.store(
          root_folder,
          server_id,
          target_date,
          new_snapshot,
          Collector.Snapshot
        )
    )

    {atom_error, {string_agg_open, _}} =
      Collector.AggServer.run(root_folder, server_id, target_date)

    assert(atom_error == :error)
    assert(string_agg_open == "Unable to open prev_date agg_server")
  end

  @tag :tmp_dir
  test "AggServer.run() generates an init agg_server table if there is no previous increment",
       %{tmp_dir: root_folder} do
    target_date = Date.utc_today()
    server_id = "https://gos.x2.europe.travian.com"

    new_player_snapshot = [
      %Collector.Snapshot{
        map_id: 20,
        x: -181,
        y: 200,
        tribe: 2,
        village_id: "v1",
        village_server_id: 19995,
        village_name: "n1",
        player_id: "p1",
        player_server_id: 361,
        player_name: "opc",
        alliance_id: "a1",
        alliance_server_id: 8,
        alliance_name: "WW",
        population: 961,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      },
      %Collector.Snapshot{
        map_id: 49,
        x: -152,
        y: 200,
        tribe: 1,
        village_id: "p2",
        village_server_id: 19702,
        village_name: "a2",
        player_id: "p2",
        player_server_id: 416,
        player_name: "opc",
        alliance_id: "a1",
        alliance_server_id: 8,
        alliance_name: "WW",
        population: 964,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      },
      %Collector.Snapshot{
        map_id: 30,
        x: -151,
        y: 180,
        tribe: 3,
        village_id: "p3",
        village_server_id: 19996,
        village_name: "a3",
        player_id: "p2",
        player_server_id: 361,
        player_name: "laskdj",
        alliance_id: "a1",
        alliance_server_id: 8,
        alliance_name: "alskj",
        population: 1200,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      },
      %Collector.Snapshot{
        map_id: 30,
        x: -151,
        y: 180,
        tribe: 4,
        village_id: "p4",
        village_server_id: 19996,
        village_name: "a3",
        player_id: "p3",
        player_server_id: 361,
        player_name: "laskdj",
        alliance_id: "a1",
        alliance_server_id: 8,
        alliance_name: "alskj",
        population: 1200,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      }
    ]

    assert(
      :ok ==
        Collector.Feed.store(
          root_folder,
          server_id,
          target_date,
          new_player_snapshot,
          Collector.Snapshot
        )
    )

    assert(:ok == Collector.AggServer.run(root_folder, server_id, target_date))

    {:ok, agg_server} =
      Collector.Feed.open(root_folder, server_id, target_date, Collector.AggServer)

    target_dt = DateTime.new!(target_date, ~T[00:00:00.000])

    assert(agg_server.target_dt == target_dt)
    assert(agg_server.server_id == server_id)
    assert(agg_server.url == server_id)
    assert(agg_server.shrink == "gos")
    assert(agg_server.speed == 2)
    assert(agg_server.region == "europe")
    assert(agg_server.estimated_starting_date == target_date)

    assert(length(agg_server.increment) == 1)
    [inc] = agg_server.increment

    expected_inc = %Collector.AggServer.Increment{
      target_dt: target_dt,
      natar_villages: 0,
      natar_population: 0,
      natar_population_variation: nil,
      total_population: 4325,
      population_variation: nil,
      total_villages: 4,
      new_villages: nil,
      removed_villages: nil,
      total_players: 3,
      new_players: nil,
      removed_players: nil,
      total_alliances: 1,
      new_alliances: nil,
      removed_alliances: nil
    }

    assert(inc == expected_inc)
  end

  @tag :tmp_dir
  test "AggPlayers.run() prepends the new increment to the increments list",
       %{tmp_dir: root_folder} do
    target_date = Date.utc_today()
    next_date = Date.add(target_date, 1)
    server_id = "https://gos.x2.europe.travian.com"

    new_player_snapshot = [
      %Collector.Snapshot{
        map_id: 20,
        x: -181,
        y: 200,
        tribe: 2,
        village_id: "v1",
        village_server_id: 19995,
        village_name: "n1",
        player_id: "p1",
        player_server_id: 361,
        player_name: "opc",
        alliance_id: "a1",
        alliance_server_id: 8,
        alliance_name: "WW",
        population: 961,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      },
      %Collector.Snapshot{
        map_id: 49,
        x: -152,
        y: 200,
        tribe: 1,
        village_id: "p2",
        village_server_id: 19702,
        village_name: "a2",
        player_id: "p2",
        player_server_id: 416,
        player_name: "opc",
        alliance_id: "a1",
        alliance_server_id: 8,
        alliance_name: "WW",
        population: 964,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      },
      %Collector.Snapshot{
        map_id: 30,
        x: -151,
        y: 180,
        tribe: 3,
        village_id: "p3",
        village_server_id: 19996,
        village_name: "a3",
        player_id: "p2",
        player_server_id: 361,
        player_name: "laskdj",
        alliance_id: "a1",
        alliance_server_id: 8,
        alliance_name: "alskj",
        population: 1200,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      },
      %Collector.Snapshot{
        map_id: 30,
        x: -151,
        y: 180,
        tribe: 4,
        village_id: "p4",
        village_server_id: 19996,
        village_name: "a3",
        player_id: "p3",
        player_server_id: 361,
        player_name: "laskdj",
        alliance_id: "a1",
        alliance_server_id: 8,
        alliance_name: "alskj",
        population: 1200,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      }
    ]

    assert(
      :ok ==
        Collector.Feed.store(
          root_folder,
          server_id,
          target_date,
          new_player_snapshot,
          Collector.Snapshot
        )
    )

    assert(:ok == Collector.AggServer.run(root_folder, server_id, target_date))

    assert(
      :ok ==
        Collector.Feed.store(
          root_folder,
          server_id,
          next_date,
          new_player_snapshot,
          Collector.Snapshot
        )
    )

    assert(:ok == Collector.AggServer.run(root_folder, server_id, next_date))

    {:ok, agg_server_next} =
      Collector.Feed.open(root_folder, server_id, next_date, Collector.AggServer)

    assert(length(agg_server_next.increment) == 2)
    [next_inc, prev_inc] = agg_server_next.increment

    assert(DateTime.compare(next_inc.target_dt, prev_inc.target_dt) == :gt)
  end

  test "increment creates a init_struct if there is no previous increment" do
    target_date = Date.utc_today()
    target_dt = DateTime.new!(target_date, ~T[00:00:00.000])

    snapshot = [
      %Collector.Snapshot{
        map_id: 27,
        x: -174,
        y: 200,
        tribe: 7,
        village_id: "https://ts30.x3.international.travian.com--V--44713",
        village_server_id: 44713,
        village_name: "05",
        player_id: "https://ts30.x3.international.travian.com--P--11436",
        player_server_id: 11436,
        player_name: "ساطور",
        alliance_id: "https://ts30.x3.international.travian.com--A--101",
        alliance_server_id: 101,
        alliance_name: "B.H",
        population: 820,
        region: nil,
        is_capital: true,
        is_city: nil,
        victory_points: nil
      },
      %Collector.Snapshot{
        map_id: 56,
        x: -145,
        y: 200,
        tribe: 6,
        village_id: "https://ts30.x3.international.travian.com--V--42229",
        village_server_id: 42229,
        village_name: "10",
        player_id: "https://ts30.x3.international.travian.com--P--1850",
        player_server_id: 1850,
        player_name: "ASTRE7",
        alliance_id: "https://ts30.x3.international.travian.com--A--33",
        alliance_server_id: 33,
        alliance_name: "PH",
        population: 843,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      },
      %Collector.Snapshot{
        map_id: 57,
        x: -144,
        y: 200,
        tribe: 6,
        village_id: "https://ts30.x3.international.travian.com--V--39161",
        village_server_id: 39161,
        village_name: "09",
        player_id: "https://ts30.x3.international.travian.com--P--1850",
        player_server_id: 1850,
        player_name: "ASTRE7",
        alliance_id: "https://ts30.x3.international.travian.com--A--33",
        alliance_server_id: 33,
        alliance_name: "PH",
        population: 830,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      },
      %Collector.Snapshot{
        map_id: 72,
        x: -129,
        y: 200,
        tribe: 3,
        village_id: "https://ts30.x3.international.travian.com--V--44176",
        village_server_id: 44176,
        village_name: "DALLA CHIESA",
        player_id: "https://ts30.x3.international.travian.com--P--10787",
        player_server_id: 10787,
        player_name: "wrosanero",
        alliance_id: "https://ts30.x3.international.travian.com--A--266",
        alliance_server_id: 266,
        alliance_name: "BDL2",
        population: 623,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      },
      %Collector.Snapshot{
        map_id: 79,
        x: -122,
        y: 200,
        tribe: 6,
        village_id: "https://ts30.x3.international.travian.com--V--48635",
        village_server_id: 48635,
        village_name: "08 Fonseca",
        player_id: "https://ts30.x3.international.travian.com--P--2265",
        player_server_id: 2265,
        player_name: "Fonseca",
        alliance_id: "https://ts30.x3.international.travian.com--A--623",
        alliance_server_id: 623,
        alliance_name: "710",
        population: 229,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      }
    ]

    expected_inc = %Collector.AggServer.Increment{
      target_dt: target_dt,
      natar_villages: 0,
      natar_population: 0,
      natar_population_variation: nil,
      total_population: 3345,
      population_variation: nil,
      total_villages: 5,
      new_villages: nil,
      removed_villages: nil,
      total_players: 4,
      new_players: nil,
      removed_players: nil,
      total_alliances: 4,
      new_alliances: nil,
      removed_alliances: nil
    }

    inc = Collector.AggServer.Increment.increment(target_dt, snapshot)
    assert(inc == expected_inc)
  end

  test "increment computes some metrics about the server" do
    target_date = Date.utc_today()
    target_dt = DateTime.new!(target_date, ~T[00:00:00.000])
    prev_target_dt = DateTime.new!(Date.add(target_date, -1), ~T[00:00:00.000])

    prev_snapshot = [
      %Collector.Snapshot{
        map_id: 27,
        x: -174,
        y: 200,
        tribe: 7,
        village_id: "https://ts30.x3.international.travian.com--V--44713",
        village_server_id: 44713,
        village_name: "05",
        player_id: "https://ts30.x3.international.travian.com--P--11436",
        player_server_id: 11436,
        player_name: "ساطور",
        alliance_id: "https://ts30.x3.international.travian.com--A--101",
        alliance_server_id: 101,
        alliance_name: "B.H",
        population: 820,
        region: nil,
        is_capital: true,
        is_city: nil,
        victory_points: nil
      },
      %Collector.Snapshot{
        map_id: 56,
        x: -145,
        y: 200,
        tribe: 6,
        village_id: "https://ts30.x3.international.travian.com--V--42229",
        village_server_id: 42229,
        village_name: "10",
        player_id: "https://ts30.x3.international.travian.com--P--1850",
        player_server_id: 1850,
        player_name: "ASTRE7",
        alliance_id: "https://ts30.x3.international.travian.com--A--33",
        alliance_server_id: 33,
        alliance_name: "PH",
        population: 843,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      },
      %Collector.Snapshot{
        map_id: 57,
        x: -144,
        y: 200,
        tribe: 6,
        village_id: "https://ts30.x3.international.travian.com--V--39161",
        village_server_id: 39161,
        village_name: "09",
        player_id: "https://ts30.x3.international.travian.com--P--1850",
        player_server_id: 1850,
        player_name: "ASTRE7",
        alliance_id: "https://ts30.x3.international.travian.com--A--33",
        alliance_server_id: 33,
        alliance_name: "PH",
        population: 830,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      },
      %Collector.Snapshot{
        map_id: 72,
        x: -129,
        y: 200,
        tribe: 3,
        village_id: "https://ts30.x3.international.travian.com--V--44176",
        village_server_id: 44176,
        village_name: "DALLA CHIESA",
        player_id: "https://ts30.x3.international.travian.com--P--10787",
        player_server_id: 10787,
        player_name: "wrosanero",
        alliance_id: "https://ts30.x3.international.travian.com--A--266",
        alliance_server_id: 266,
        alliance_name: "BDL2",
        population: 623,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      },
      %Collector.Snapshot{
        map_id: 79,
        x: -122,
        y: 200,
        tribe: 6,
        village_id: "https://ts30.x3.international.travian.com--V--48635",
        village_server_id: 48635,
        village_name: "08 Fonseca",
        player_id: "https://ts30.x3.international.travian.com--P--2265",
        player_server_id: 2265,
        player_name: "Fonseca",
        alliance_id: "https://ts30.x3.international.travian.com--A--623",
        alliance_server_id: 623,
        alliance_name: "710",
        population: 229,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      }
    ]

    new_snapshot = [
      %Collector.Snapshot{
        map_id: 27,
        x: -174,
        y: 200,
        tribe: 7,
        village_id: "https://ts30.x3.international.travian.com--V--44713",
        village_server_id: 44713,
        village_name: "05",
        player_id: "https://ts30.x3.international.travian.com--P--11436",
        player_server_id: 11436,
        player_name: "ساطور",
        alliance_id: "https://ts30.x3.international.travian.com--A--101",
        alliance_server_id: 101,
        alliance_name: "B.H",
        population: 820,
        region: nil,
        is_capital: true,
        is_city: nil,
        victory_points: nil
      },
      %Collector.Snapshot{
        map_id: 56,
        x: -145,
        y: 200,
        tribe: 6,
        village_id: "https://ts30.x3.international.travian.com--V--42229",
        village_server_id: 42229,
        village_name: "10",
        player_id: "https://ts30.x3.international.travian.com--P--1850",
        player_server_id: 1850,
        player_name: "ASTRE7",
        alliance_id: "https://ts30.x3.international.travian.com--A--33",
        alliance_server_id: 33,
        alliance_name: "PH",
        population: 843,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      },
      %Collector.Snapshot{
        map_id: 57,
        x: -144,
        y: 200,
        tribe: 6,
        village_id: "https://ts30.x3.international.travian.com--V--39161",
        village_server_id: 39161,
        village_name: "09",
        player_id: "https://ts30.x3.international.travian.com--P--1850",
        player_server_id: 1850,
        player_name: "ASTRE7",
        alliance_id: "https://ts30.x3.international.travian.com--A--33",
        alliance_server_id: 33,
        alliance_name: "PH",
        population: 830,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      },
      %Collector.Snapshot{
        map_id: 72,
        x: -129,
        y: 200,
        tribe: 3,
        village_id: "https://ts30.x3.international.travian.com--V--44176",
        village_server_id: 44176,
        village_name: "DALLA CHIESA",
        player_id: "https://ts30.x3.international.travian.com--P--10787",
        player_server_id: 10787,
        player_name: "wrosanero",
        alliance_id: "https://ts30.x3.international.travian.com--A--266",
        alliance_server_id: 266,
        alliance_name: "BDL2",
        population: 623,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      },
      %Collector.Snapshot{
        map_id: 83,
        x: -118,
        y: 200,
        tribe: 6,
        village_id: "https://ts30.x3.international.travian.com--V--47143",
        village_server_id: 47143,
        village_name: "07 Fonseca",
        player_id: "https://ts30.x3.international.travian.com--P--2265",
        player_server_id: 2265,
        player_name: "Fonseca",
        alliance_id: "https://ts30.x3.international.travian.com--A--623",
        alliance_server_id: 623,
        alliance_name: "710",
        population: 414,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      },
      %Collector.Snapshot{
        map_id: 115,
        x: -86,
        y: 200,
        tribe: 1,
        village_id: "https://ts30.x3.international.travian.com--V--39368",
        village_server_id: 39368,
        village_name: "02",
        player_id: "https://ts30.x3.international.travian.com--P--9808",
        player_server_id: 9808,
        player_name: "Aeirdun",
        alliance_id: "https://ts30.x3.international.travian.com--A--93",
        alliance_server_id: 93,
        alliance_name: "SWU",
        population: 838,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      }
    ]

    expected_inc = %Collector.AggServer.Increment{
      target_dt: target_dt,
      natar_villages: 0,
      natar_population: 0,
      natar_population_variation: 0,
      total_population: 4368,
      population_variation: 1023,
      total_villages: 6,
      new_villages: 2,
      removed_villages: 1,
      total_players: 5,
      new_players: 1,
      removed_players: 0,
      total_alliances: 5,
      new_alliances: 1,
      removed_alliances: 0
    }

    prev_inc = Collector.AggServer.Increment.increment(prev_target_dt, prev_snapshot)

    inc =
      Collector.AggServer.Increment.increment(target_dt, new_snapshot, prev_snapshot, prev_inc)

    assert(inc == expected_inc)
  end

  test "increment computes the metrics properly when there are natars" do
    target_date = Date.utc_today()
    target_dt = DateTime.new!(target_date, ~T[00:00:00.000])
    prev_target_dt = DateTime.new!(Date.add(target_date, -1), ~T[00:00:00.000])

    prev_snapshot = [
      %Collector.Snapshot{
        map_id: 27,
        x: -174,
        y: 200,
        tribe: 7,
        village_id: "https://ts30.x3.international.travian.com--V--44713",
        village_server_id: 44713,
        village_name: "05",
        player_id: "https://ts30.x3.international.travian.com--P--11436",
        player_server_id: 11436,
        player_name: "ساطور",
        alliance_id: "https://ts30.x3.international.travian.com--A--101",
        alliance_server_id: 101,
        alliance_name: "B.H",
        population: 820,
        region: nil,
        is_capital: true,
        is_city: nil,
        victory_points: nil
      },
      %Collector.Snapshot{
        map_id: 56,
        x: -145,
        y: 200,
        tribe: 6,
        village_id: "https://ts30.x3.international.travian.com--V--42229",
        village_server_id: 42229,
        village_name: "10",
        player_id: "https://ts30.x3.international.travian.com--P--1850",
        player_server_id: 1850,
        player_name: "ASTRE7",
        alliance_id: "https://ts30.x3.international.travian.com--A--33",
        alliance_server_id: 33,
        alliance_name: "PH",
        population: 843,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      },
      %Collector.Snapshot{
        map_id: 57,
        x: -144,
        y: 200,
        tribe: 6,
        village_id: "https://ts30.x3.international.travian.com--V--39161",
        village_server_id: 39161,
        village_name: "09",
        player_id: "https://ts30.x3.international.travian.com--P--1850",
        player_server_id: 1850,
        player_name: "ASTRE7",
        alliance_id: "https://ts30.x3.international.travian.com--A--33",
        alliance_server_id: 33,
        alliance_name: "PH",
        population: 830,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      },
      %Collector.Snapshot{
        map_id: 72,
        x: -129,
        y: 200,
        tribe: 3,
        village_id: "https://ts30.x3.international.travian.com--V--44176",
        village_server_id: 44176,
        village_name: "DALLA CHIESA",
        player_id: "https://ts30.x3.international.travian.com--P--10787",
        player_server_id: 10787,
        player_name: "wrosanero",
        alliance_id: "https://ts30.x3.international.travian.com--A--266",
        alliance_server_id: 266,
        alliance_name: "BDL2",
        population: 623,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      },
      %Collector.Snapshot{
        map_id: 79,
        x: -122,
        y: 200,
        tribe: 6,
        village_id: "https://ts30.x3.international.travian.com--V--48635",
        village_server_id: 48635,
        village_name: "08 Fonseca",
        player_id: "https://ts30.x3.international.travian.com--P--2265",
        player_server_id: 2265,
        player_name: "Fonseca",
        alliance_id: "https://ts30.x3.international.travian.com--A--623",
        alliance_server_id: 623,
        alliance_name: "710",
        population: 229,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      },
      %Collector.Snapshot{
        map_id: 6083,
        x: -133,
        y: 185,
        tribe: 5,
        village_id: "https://ts30.x3.international.travian.com--V--31936",
        village_server_id: 31936,
        village_name: "Natars -133|185",
        player_id: "https://ts30.x3.international.travian.com--P--1",
        player_server_id: 1,
        player_name: "Natars",
        alliance_id: "https://ts30.x3.international.travian.com--A--0",
        alliance_server_id: 0,
        alliance_name: "",
        population: 212,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      },
      %Collector.Snapshot{
        map_id: 16372,
        x: 131,
        y: 160,
        tribe: 5,
        village_id: "https://ts30.x3.international.travian.com--V--39810",
        village_server_id: 39810,
        village_name: "Natars 131|160",
        player_id: "https://ts30.x3.international.travian.com--P--1",
        player_server_id: 1,
        player_name: "Natars",
        alliance_id: "https://ts30.x3.international.travian.com--A--0",
        alliance_server_id: 0,
        alliance_name: "",
        population: 77,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      }
    ]

    new_snapshot = [
      %Collector.Snapshot{
        map_id: 27,
        x: -174,
        y: 200,
        tribe: 7,
        village_id: "https://ts30.x3.international.travian.com--V--44713",
        village_server_id: 44713,
        village_name: "05",
        player_id: "https://ts30.x3.international.travian.com--P--11436",
        player_server_id: 11436,
        player_name: "ساطور",
        alliance_id: "https://ts30.x3.international.travian.com--A--101",
        alliance_server_id: 101,
        alliance_name: "B.H",
        population: 820,
        region: nil,
        is_capital: true,
        is_city: nil,
        victory_points: nil
      },
      %Collector.Snapshot{
        map_id: 56,
        x: -145,
        y: 200,
        tribe: 6,
        village_id: "https://ts30.x3.international.travian.com--V--42229",
        village_server_id: 42229,
        village_name: "10",
        player_id: "https://ts30.x3.international.travian.com--P--1850",
        player_server_id: 1850,
        player_name: "ASTRE7",
        alliance_id: "https://ts30.x3.international.travian.com--A--33",
        alliance_server_id: 33,
        alliance_name: "PH",
        population: 843,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      },
      %Collector.Snapshot{
        map_id: 57,
        x: -144,
        y: 200,
        tribe: 6,
        village_id: "https://ts30.x3.international.travian.com--V--39161",
        village_server_id: 39161,
        village_name: "09",
        player_id: "https://ts30.x3.international.travian.com--P--1850",
        player_server_id: 1850,
        player_name: "ASTRE7",
        alliance_id: "https://ts30.x3.international.travian.com--A--33",
        alliance_server_id: 33,
        alliance_name: "PH",
        population: 830,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      },
      %Collector.Snapshot{
        map_id: 72,
        x: -129,
        y: 200,
        tribe: 3,
        village_id: "https://ts30.x3.international.travian.com--V--44176",
        village_server_id: 44176,
        village_name: "DALLA CHIESA",
        player_id: "https://ts30.x3.international.travian.com--P--10787",
        player_server_id: 10787,
        player_name: "wrosanero",
        alliance_id: "https://ts30.x3.international.travian.com--A--266",
        alliance_server_id: 266,
        alliance_name: "BDL2",
        population: 623,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      },
      %Collector.Snapshot{
        map_id: 83,
        x: -118,
        y: 200,
        tribe: 6,
        village_id: "https://ts30.x3.international.travian.com--V--47143",
        village_server_id: 47143,
        village_name: "07 Fonseca",
        player_id: "https://ts30.x3.international.travian.com--P--2265",
        player_server_id: 2265,
        player_name: "Fonseca",
        alliance_id: "https://ts30.x3.international.travian.com--A--623",
        alliance_server_id: 623,
        alliance_name: "710",
        population: 414,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      },
      %Collector.Snapshot{
        map_id: 115,
        x: -86,
        y: 200,
        tribe: 1,
        village_id: "https://ts30.x3.international.travian.com--V--39368",
        village_server_id: 39368,
        village_name: "02",
        player_id: "https://ts30.x3.international.travian.com--P--9808",
        player_server_id: 9808,
        player_name: "Aeirdun",
        alliance_id: "https://ts30.x3.international.travian.com--A--93",
        alliance_server_id: 93,
        alliance_name: "SWU",
        population: 838,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      },
      %Collector.Snapshot{
        map_id: 6083,
        x: -133,
        y: 185,
        tribe: 5,
        village_id: "https://ts30.x3.international.travian.com--V--31936",
        village_server_id: 31936,
        village_name: "Natars -133|185",
        player_id: "https://ts30.x3.international.travian.com--P--1",
        player_server_id: 1,
        player_name: "Natars",
        alliance_id: "https://ts30.x3.international.travian.com--A--0",
        alliance_server_id: 0,
        alliance_name: "",
        population: 300,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      },
      %Collector.Snapshot{
        map_id: 16372,
        x: 131,
        y: 160,
        tribe: 5,
        village_id: "https://ts30.x3.international.travian.com--V--39810",
        village_server_id: 39810,
        village_name: "Natars 131|160",
        player_id: "https://ts30.x3.international.travian.com--P--1",
        player_server_id: 1,
        player_name: "Natars",
        alliance_id: "https://ts30.x3.international.travian.com--A--0",
        alliance_server_id: 0,
        alliance_name: "",
        population: 77,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      }
    ]

    expected_prev_inc = %Collector.AggServer.Increment{
      target_dt: prev_target_dt,
      natar_villages: 2,
      natar_population: 289,
      natar_population_variation: nil,
      total_population: 3345,
      population_variation: nil,
      total_villages: 5,
      new_villages: nil,
      removed_villages: nil,
      total_players: 4,
      new_players: nil,
      removed_players: nil,
      total_alliances: 4,
      new_alliances: nil,
      removed_alliances: nil
    }

    expected_inc = %Collector.AggServer.Increment{
      target_dt: target_dt,
      natar_villages: 2,
      natar_population: 377,
      natar_population_variation: 88,
      total_population: 4368,
      population_variation: 1023,
      total_villages: 6,
      new_villages: 2,
      removed_villages: 1,
      total_players: 5,
      new_players: 1,
      removed_players: 0,
      total_alliances: 5,
      new_alliances: 1,
      removed_alliances: 0
    }

    prev_inc = Collector.AggServer.Increment.increment(prev_target_dt, prev_snapshot)

    inc =
      Collector.AggServer.Increment.increment(target_dt, new_snapshot, prev_snapshot, prev_inc)

    assert(prev_inc == expected_prev_inc)
    assert(inc == expected_inc)
  end
end
