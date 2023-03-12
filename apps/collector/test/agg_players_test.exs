defmodule Collector.AggPlayersTest do
  use ExUnit.Case

  test "AggPlayers.common_villages() return a map with player_id as key and a list of village_ids that are owned on the new snapshot or were owned in the previous snapshot by the player" do
    new_snapshot = [
      %Collector.SnapshotRow{
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
      %Collector.SnapshotRow{
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
      %Collector.SnapshotRow{
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
      %Collector.SnapshotRow{
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
        population: 100,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      }
    ]

    prev_snapshot = [
      %Collector.SnapshotRow{
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
      %Collector.SnapshotRow{
        map_id: 49,
        x: -152,
        y: 200,
        tribe: 1,
        village_id: "p2",
        village_server_id: 19702,
        village_name: "a2",
        player_id: "p1",
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
      %Collector.SnapshotRow{
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
      %Collector.SnapshotRow{
        map_id: 30,
        x: -151,
        y: 180,
        tribe: 4,
        village_id: "p5",
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
      }
    ]

    output = Collector.AggPlayers.common_villages(new_snapshot, prev_snapshot)

    assert(Map.has_key?(output, "p1"))
    assert(Map.fetch!(output, "p1") |> Enum.sort() == ["p2", "v1"])
    assert(Map.has_key?(output, "p2"))
    assert(Map.fetch!(output, "p2") |> Enum.sort() == ["p2", "p3", "p5"])
    assert(Map.has_key?(output, "p3"))
    assert(Map.fetch!(output, "p3") |> Enum.sort() == ["p4"])
  end

  @tag :tmp_dir
  test "AggPlayers.run() returns unable to open the file if there is no snapshot of target_date",
       %{tmp_dir: root_folder} do
    target_date = Date.utc_today()
    server_id = "server1"

    {atom_error, {string_agg_open, _}} =
      Collector.AggPlayers.run(root_folder, server_id, target_date)

    assert(atom_error == :error)
    assert(string_agg_open == "Unable to open target_date snapshot")
  end

  @tag :tmp_dir
  test "AggPlayers.run() if there is previous snapshot but not previous agg_players returns error",
       %{tmp_dir: root_folder} do
    target_date = Date.utc_today()
    prev_date = Date.add(target_date, -1)
    server_id = "server1"

    new_player_snapshot = [
      %Collector.SnapshotRow{
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
      %Collector.SnapshotRow{
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
      %Collector.SnapshotRow{
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
      %Collector.SnapshotRow{
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
      :ok == Collector.Snapshot.store(root_folder, server_id, new_player_snapshot, prev_date)
    )

    assert(
      :ok == Collector.Snapshot.store(root_folder, server_id, new_player_snapshot, target_date)
    )

    {atom_error, {string_agg_open, _}} =
      Collector.AggPlayers.run(root_folder, server_id, target_date)

    assert(atom_error == :error)
    assert(string_agg_open == "Unable to open prev_date agg_players")
  end

  @tag :tmp_dir
  test "AggPlayers.run() generates an init agg_players table if there is no previous increment",
       %{tmp_dir: root_folder} do
    target_date = Date.utc_today()
    server_id = "server1"

    new_player_snapshot = [
      %Collector.SnapshotRow{
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
      %Collector.SnapshotRow{
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
      %Collector.SnapshotRow{
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
      %Collector.SnapshotRow{
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

    encoded_snapshot = Collector.snapshot_to_format(new_player_snapshot)

    assert(
      :ok ==
        Storage.store(
          root_folder,
          server_id,
          Collector.snapshot_options(),
          encoded_snapshot,
          target_date
        )
    )

    assert(:ok == Collector.AggPlayers.run(root_folder, server_id, target_date))
    {:ok, agg_players} = Collector.AggPlayers.open(root_folder, server_id, target_date)
    for agg_player <- agg_players, do: assert(is_struct(agg_player, Collector.AggPlayers))
  end

  @tag :tmp_dir
  test "AggPlayers.run() generates an agg_players table using the last increment", %{
    tmp_dir: root_folder
  } do
    target_date = Date.utc_today()
    yesterday = Date.add(target_date, -1)
    server_id = "server1"

    new_player_snapshot = [
      %Collector.SnapshotRow{
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
      %Collector.SnapshotRow{
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
      %Collector.SnapshotRow{
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
      %Collector.SnapshotRow{
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

    prev_player_snapshot = [
      %Collector.SnapshotRow{
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
      %Collector.SnapshotRow{
        map_id: 49,
        x: -152,
        y: 200,
        tribe: 2,
        village_id: "p2",
        village_server_id: 19702,
        village_name: "a2",
        player_id: "p1",
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
      %Collector.SnapshotRow{
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
        alliance_id: "a2",
        alliance_server_id: 8,
        alliance_name: "alskj",
        population: 1200,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      }
    ]

    new_encoded_snapshot = Collector.snapshot_to_format(new_player_snapshot)
    prev_encoded_snapshot = Collector.snapshot_to_format(prev_player_snapshot)

    assert(
      :ok ==
        Storage.store(
          root_folder,
          server_id,
          Collector.snapshot_options(),
          prev_encoded_snapshot,
          yesterday
        )
    )

    assert(:ok == Collector.AggPlayers.run(root_folder, server_id, yesterday))

    assert(
      :ok ==
        Storage.store(
          root_folder,
          server_id,
          Collector.snapshot_options(),
          new_encoded_snapshot,
          target_date
        )
    )

    assert(:ok == Collector.AggPlayers.run(root_folder, server_id, target_date))
    {:ok, agg_players} = Collector.AggPlayers.open(root_folder, server_id, target_date)

    [p1, p2, p3] = Enum.sort_by(agg_players, & &1.player_id)

    assert(length(p1.increment) == 2)
    assert(length(p2.increment) == 2)
    assert(length(p3.increment) == 1)
  end

  @tag :tmp_dir
  test "AggPlayers.run() while generating the new increment from a previous one, we conserve the players that don't appear in the new increment just updating the target_dt",
       %{
         tmp_dir: root_folder
       } do
    target_date = Date.utc_today()
    yesterday = Date.add(target_date, -1)
    server_id = "server1"

    new_player_snapshot = [
      %Collector.SnapshotRow{
        map_id: 20,
        x: -181,
        y: 200,
        tribe: 2,
        village_id: "v1",
        village_server_id: 19995,
        village_name: "n1",
        player_id: "p2",
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
      %Collector.SnapshotRow{
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
      }
    ]

    prev_player_snapshot = [
      %Collector.SnapshotRow{
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
      %Collector.SnapshotRow{
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
        alliance_id: "a2",
        alliance_server_id: 8,
        alliance_name: "alskj",
        population: 1200,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      }
    ]

    new_encoded_snapshot = Collector.snapshot_to_format(new_player_snapshot)
    prev_encoded_snapshot = Collector.snapshot_to_format(prev_player_snapshot)

    assert(
      :ok ==
        Storage.store(
          root_folder,
          server_id,
          Collector.snapshot_options(),
          prev_encoded_snapshot,
          yesterday
        )
    )

    assert(:ok == Collector.AggPlayers.run(root_folder, server_id, yesterday))

    assert(
      :ok ==
        Storage.store(
          root_folder,
          server_id,
          Collector.snapshot_options(),
          new_encoded_snapshot,
          target_date
        )
    )

    assert(:ok == Collector.AggPlayers.run(root_folder, server_id, target_date))
    {:ok, agg_players} = Collector.AggPlayers.open(root_folder, server_id, target_date)

    [p1, p2] = Enum.sort_by(agg_players, & &1.player_id)

    assert(length(p1.increment) == 1)
    assert(Date.compare(DateTime.to_date(p1.target_dt), target_date) == :eq)
    assert(length(p2.increment) == 2)
  end

  @tag :tmp_dir
  test "AggPlayers.run() while generating the new increment from a previous one, we add the players that are new by creating and init Collector.Aggplayers struct",
       %{
         tmp_dir: root_folder
       } do
    target_date = Date.utc_today()
    yesterday = Date.add(target_date, -1)
    server_id = "server1"

    new_player_snapshot = [
      %Collector.SnapshotRow{
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
      %Collector.SnapshotRow{
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
      %Collector.SnapshotRow{
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
      %Collector.SnapshotRow{
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

    prev_player_snapshot = [
      %Collector.SnapshotRow{
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
      %Collector.SnapshotRow{
        map_id: 49,
        x: -152,
        y: 200,
        tribe: 2,
        village_id: "p2",
        village_server_id: 19702,
        village_name: "a2",
        player_id: "p1",
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
      %Collector.SnapshotRow{
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
        alliance_id: "a2",
        alliance_server_id: 8,
        alliance_name: "alskj",
        population: 1200,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      }
    ]

    new_encoded_snapshot = Collector.snapshot_to_format(new_player_snapshot)
    prev_encoded_snapshot = Collector.snapshot_to_format(prev_player_snapshot)

    assert(
      :ok ==
        Storage.store(
          root_folder,
          server_id,
          Collector.snapshot_options(),
          prev_encoded_snapshot,
          yesterday
        )
    )

    assert(:ok == Collector.AggPlayers.run(root_folder, server_id, yesterday))

    assert(
      :ok ==
        Storage.store(
          root_folder,
          server_id,
          Collector.snapshot_options(),
          new_encoded_snapshot,
          target_date
        )
    )

    assert(:ok == Collector.AggPlayers.run(root_folder, server_id, target_date))
    {:ok, agg_players} = Collector.AggPlayers.open(root_folder, server_id, target_date)

    [_p1, _p2, p3] = Enum.sort_by(agg_players, & &1.player_id)

    assert(Date.compare(DateTime.to_date(p3.target_dt), target_date) == :eq)

    copied_target_dt = p3.target_dt

    assert(p3.server_id == server_id)
    assert(p3.player_id == "p3")
    assert(p3.estimated_starting_date == target_date)
    assert(p3.estimated_tribe == 4)
    assert(length(p3.increment) == 1)

    expected_p3_increment = %Collector.AggPlayers.Increment{
      target_dt: copied_target_dt,
      total_population: 1200,
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

    assert(hd(p3.increment) == expected_p3_increment)
  end

  @tag :tmp_dir
  test "AggPlayers.run() generates an agg_players table even if a previous snapshot and/or previous increment is missing, it compute the difference between the 2 lastest snapshots",
       %{tmp_dir: root_folder} do
    target_date = Date.utc_today()
    yesterday2 = Date.add(target_date, -2)
    server_id = "server1"

    new_player_snapshot = [
      %Collector.SnapshotRow{
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
      %Collector.SnapshotRow{
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
      %Collector.SnapshotRow{
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
      %Collector.SnapshotRow{
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

    prev_player_snapshot = [
      %Collector.SnapshotRow{
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
      %Collector.SnapshotRow{
        map_id: 49,
        x: -152,
        y: 200,
        tribe: 2,
        village_id: "p2",
        village_server_id: 19702,
        village_name: "a2",
        player_id: "p1",
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
      %Collector.SnapshotRow{
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
        alliance_id: "a2",
        alliance_server_id: 8,
        alliance_name: "alskj",
        population: 1200,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      }
    ]

    new_encoded_snapshot = Collector.snapshot_to_format(new_player_snapshot)
    prev_encoded_snapshot = Collector.snapshot_to_format(prev_player_snapshot)

    assert(
      :ok ==
        Storage.store(
          root_folder,
          server_id,
          Collector.snapshot_options(),
          prev_encoded_snapshot,
          yesterday2
        )
    )

    assert(:ok == Collector.AggPlayers.run(root_folder, server_id, yesterday2))

    assert(
      :ok ==
        Storage.store(
          root_folder,
          server_id,
          Collector.snapshot_options(),
          new_encoded_snapshot,
          target_date
        )
    )

    assert(:ok == Collector.AggPlayers.run(root_folder, server_id, target_date))
    {:ok, agg_players} = Collector.AggPlayers.open(root_folder, server_id, target_date)

    [p1, p2, p3] = Enum.sort_by(agg_players, & &1.player_id)

    assert(length(p1.increment) == 2)
    assert(hd(p1.increment).population_decrease_by_conquered == 964)
    assert(length(p2.increment) == 2)
    assert(hd(p2.increment).population_increase_by_conquered == 964)
    assert(length(p3.increment) == 1)
    assert(hd(p3.increment).population_increase_by_conquered == nil)
  end

  test "AggPlayers.process() generate a new increment for no new players or create an init for the new ones" do
    target_dt = DateTime.utc_now()
    new_target_dt = DateTime.add(target_dt, 3600 * 24)
    server_id = "server1"

    new_player_snapshot = [
      %Collector.SnapshotRow{
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
      %Collector.SnapshotRow{
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
      %Collector.SnapshotRow{
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
      %Collector.SnapshotRow{
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

    prev_player_snapshot = [
      %Collector.SnapshotRow{
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
      %Collector.SnapshotRow{
        map_id: 49,
        x: -152,
        y: 200,
        tribe: 2,
        village_id: "p2",
        village_server_id: 19702,
        village_name: "a2",
        player_id: "p1",
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
      %Collector.SnapshotRow{
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
        alliance_id: "a2",
        alliance_server_id: 8,
        alliance_name: "alskj",
        population: 1200,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      }
    ]

    init_agg_players = Collector.AggPlayers.process(target_dt, server_id, prev_player_snapshot)

    new_agg_players =
      [p1, p2, p3] =
      Collector.AggPlayers.process(
        new_target_dt,
        server_id,
        new_player_snapshot,
        prev_player_snapshot,
        init_agg_players
      )
      |> Enum.sort_by(& &1.player_id)

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
    server_id = "server1"

    new_player_snapshot = [
      %Collector.SnapshotRow{
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
      %Collector.SnapshotRow{
        map_id: 49,
        x: -152,
        y: 200,
        tribe: 2,
        village_id: "p2",
        village_server_id: 19702,
        village_name: "a2",
        player_id: "p1",
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
      %Collector.SnapshotRow{
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
        alliance_id: "a2",
        alliance_server_id: 8,
        alliance_name: "alskj",
        population: 1200,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      }
    ]

    init_agg_players =
      [p1, p2] =
      Collector.AggPlayers.process(target_dt, server_id, new_player_snapshot)
      |> Enum.sort_by(& &1.player_id)

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
      %Collector.SnapshotRow{
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
      %Collector.SnapshotRow{
        map_id: 49,
        x: -152,
        y: 200,
        tribe: 1,
        village_id: "p2",
        village_server_id: 19702,
        village_name: "a2",
        player_id: "p1",
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
      %Collector.SnapshotRow{
        map_id: 30,
        x: -151,
        y: 180,
        tribe: 3,
        village_id: "p3",
        village_server_id: 19996,
        village_name: "a3",
        player_id: "p1",
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

    prev_player_snapshot = [
      %Collector.SnapshotRow{
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
      %Collector.SnapshotRow{
        map_id: 49,
        x: -152,
        y: 200,
        tribe: 1,
        village_id: "p2",
        village_server_id: 19702,
        village_name: "a2",
        player_id: "p1",
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
      %Collector.SnapshotRow{
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
        alliance_id: "a2",
        alliance_server_id: 8,
        alliance_name: "alskj",
        population: 1200,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      }
    ]

    inc =
      Collector.AggPlayers.Increment.increment(
        target_dt,
        player_id,
        new_player_snapshot,
        prev_player_snapshot
      )

    assert(is_struct(inc, Collector.AggPlayers.Increment))
    assert(inc.target_dt == target_dt)
    assert(inc.new_village_conquered == 1)
  end

  test "AggPlayers.increment() defines a village as founded if it is new for the player was and it wasn't owned by another player in the previous snapshot" do
    target_dt = DateTime.utc_now()
    player_id = "p1"

    new_player_snapshot = [
      %Collector.SnapshotRow{
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
      %Collector.SnapshotRow{
        map_id: 49,
        x: -152,
        y: 200,
        tribe: 1,
        village_id: "p2",
        village_server_id: 19702,
        village_name: "a2",
        player_id: "p1",
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
      %Collector.SnapshotRow{
        map_id: 30,
        x: -151,
        y: 180,
        tribe: 3,
        village_id: "p3",
        village_server_id: 19996,
        village_name: "a3",
        player_id: "p1",
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

    prev_player_snapshot = [
      %Collector.SnapshotRow{
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
      %Collector.SnapshotRow{
        map_id: 49,
        x: -152,
        y: 200,
        tribe: 1,
        village_id: "p2",
        village_server_id: 19702,
        village_name: "a2",
        player_id: "p1",
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
      }
    ]

    inc =
      Collector.AggPlayers.Increment.increment(
        target_dt,
        player_id,
        new_player_snapshot,
        prev_player_snapshot
      )

    assert(is_struct(inc, Collector.AggPlayers.Increment))
    assert(inc.target_dt == target_dt)
    assert(inc.new_village_founded == 1)
  end

  test "AggPlayers.increment() defines a village as lost_destroyed if it doesn't appear in the new snapshot and it was owned by the player in the previous snapshot" do
    target_dt = DateTime.utc_now()
    player_id = "p1"

    new_player_snapshot = [
      %Collector.SnapshotRow{
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
      }
    ]

    prev_player_snapshot = [
      %Collector.SnapshotRow{
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
      %Collector.SnapshotRow{
        map_id: 49,
        x: -152,
        y: 200,
        tribe: 1,
        village_id: "p2",
        village_server_id: 19702,
        village_name: "a2",
        player_id: "p1",
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
      %Collector.SnapshotRow{
        map_id: 30,
        x: -151,
        y: 180,
        tribe: 3,
        village_id: "p3",
        village_server_id: 19996,
        village_name: "a3",
        player_id: "p1",
        player_server_id: 361,
        player_name: "laskdj",
        alliance_id: "a2",
        alliance_server_id: 8,
        alliance_name: "alskj",
        population: 1200,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      }
    ]

    inc =
      Collector.AggPlayers.Increment.increment(
        target_dt,
        player_id,
        new_player_snapshot,
        prev_player_snapshot
      )

    assert(is_struct(inc, Collector.AggPlayers.Increment))
    assert(inc.target_dt == target_dt)
    assert(inc.lost_village_destroyed == 2)
  end

  test "AggPlayers.increment() defines a village as lost_conquered if it is owned by another player in the new snapshot" do
    target_dt = DateTime.utc_now()
    player_id = "p1"

    new_player_snapshot = [
      %Collector.SnapshotRow{
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
      %Collector.SnapshotRow{
        map_id: 30,
        x: -151,
        y: 180,
        tribe: 3,
        village_id: "p3",
        village_server_id: 19996,
        village_name: "a3",
        player_id: "p1",
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

    prev_player_snapshot = [
      %Collector.SnapshotRow{
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
      %Collector.SnapshotRow{
        map_id: 49,
        x: -152,
        y: 200,
        tribe: 1,
        village_id: "p2",
        village_server_id: 19702,
        village_name: "a2",
        player_id: "p1",
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
      %Collector.SnapshotRow{
        map_id: 30,
        x: -151,
        y: 180,
        tribe: 3,
        village_id: "p3",
        village_server_id: 19996,
        village_name: "a3",
        player_id: "p1",
        player_server_id: 361,
        player_name: "laskdj",
        alliance_id: "a2",
        alliance_server_id: 8,
        alliance_name: "alskj",
        population: 1200,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      }
    ]

    inc =
      Collector.AggPlayers.Increment.increment(
        target_dt,
        player_id,
        new_player_snapshot,
        prev_player_snapshot
      )

    assert(is_struct(inc, Collector.AggPlayers.Increment))
    assert(inc.target_dt == target_dt)
    assert(inc.lost_village_conquered == 1)
  end

  test "AggPlayers.increment() defines total_villages as the count of the villages owned by the player in the new snapshot " do
    target_dt = DateTime.utc_now()
    player_id = "p1"

    new_player_snapshot = [
      %Collector.SnapshotRow{
        map_id: 20,
        x: -181,
        y: 200,
        tribe: 2,
        village_id: "v1",
        village_server_id: 19995,
        village_name: "n1",
        player_id: "p2",
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
      %Collector.SnapshotRow{
        map_id: 49,
        x: -152,
        y: 200,
        tribe: 1,
        village_id: "p2",
        village_server_id: 19702,
        village_name: "a2",
        player_id: "p1",
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
      %Collector.SnapshotRow{
        map_id: 30,
        x: -151,
        y: 180,
        tribe: 3,
        village_id: "p3",
        village_server_id: 19996,
        village_name: "a3",
        player_id: "p1",
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

    prev_player_snapshot = [
      %Collector.SnapshotRow{
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
      %Collector.SnapshotRow{
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
        alliance_id: "a2",
        alliance_server_id: 8,
        alliance_name: "alskj",
        population: 1200,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      }
    ]

    inc =
      Collector.AggPlayers.Increment.increment(
        target_dt,
        player_id,
        new_player_snapshot,
        prev_player_snapshot
      )

    assert(is_struct(inc, Collector.AggPlayers.Increment))
    assert(inc.target_dt == target_dt)
    assert(inc.total_villages == 2)
  end

  test "AggPlayers.increment() defines total_population as the sum of village's populations owned by the player in the new snapshot" do
    target_dt = DateTime.utc_now()
    player_id = "p1"

    new_player_snapshot = [
      %Collector.SnapshotRow{
        map_id: 20,
        x: -181,
        y: 200,
        tribe: 2,
        village_id: "v1",
        village_server_id: 19995,
        village_name: "n1",
        player_id: "p2",
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
      %Collector.SnapshotRow{
        map_id: 30,
        x: -151,
        y: 180,
        tribe: 3,
        village_id: "p3",
        village_server_id: 19996,
        village_name: "a3",
        player_id: "p1",
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
      %Collector.SnapshotRow{
        map_id: 30,
        x: -151,
        y: 180,
        tribe: 3,
        village_id: "v5",
        village_server_id: 19996,
        village_name: "a3",
        player_id: "p1",
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

    prev_player_snapshot = [
      %Collector.SnapshotRow{
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
      %Collector.SnapshotRow{
        map_id: 49,
        x: -152,
        y: 200,
        tribe: 1,
        village_id: "p2",
        village_server_id: 19702,
        village_name: "a2",
        player_id: "p1",
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
      %Collector.SnapshotRow{
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
        alliance_id: "a2",
        alliance_server_id: 8,
        alliance_name: "alskj",
        population: 1200,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      }
    ]

    inc =
      Collector.AggPlayers.Increment.increment(
        target_dt,
        player_id,
        new_player_snapshot,
        prev_player_snapshot
      )

    assert(is_struct(inc, Collector.AggPlayers.Increment))
    assert(inc.target_dt == target_dt)
    assert(inc.total_population == 2400)
  end

  test "AggPlayers.increment() defines population_increase as the sum of village's populations increment if this increment is positive" do
    target_dt = DateTime.utc_now()
    player_id = "p1"

    new_player_snapshot = [
      %Collector.SnapshotRow{
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
        population: 900,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      },
      %Collector.SnapshotRow{
        map_id: 49,
        x: -152,
        y: 200,
        tribe: 1,
        village_id: "p2",
        village_server_id: 19702,
        village_name: "a2",
        player_id: "p1",
        player_server_id: 416,
        player_name: "opc",
        alliance_id: "a1",
        alliance_server_id: 8,
        alliance_name: "WW",
        population: 1000,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      },
      %Collector.SnapshotRow{
        map_id: 30,
        x: -151,
        y: 180,
        tribe: 3,
        village_id: "p3",
        village_server_id: 19996,
        village_name: "a3",
        player_id: "p1",
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

    prev_player_snapshot = [
      %Collector.SnapshotRow{
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
      %Collector.SnapshotRow{
        map_id: 49,
        x: -152,
        y: 200,
        tribe: 1,
        village_id: "p2",
        village_server_id: 19702,
        village_name: "a2",
        player_id: "p1",
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
      }
    ]

    inc =
      Collector.AggPlayers.Increment.increment(
        target_dt,
        player_id,
        new_player_snapshot,
        prev_player_snapshot
      )

    assert(is_struct(inc, Collector.AggPlayers.Increment))
    assert(inc.target_dt == target_dt)
    assert(inc.population_increase == 36)
  end

  test "AggPlayers.increment() defines population_increase_by_founded as the sum of founded village's populations in new snapshot" do
    target_dt = DateTime.utc_now()
    player_id = "p1"

    new_player_snapshot = [
      %Collector.SnapshotRow{
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
      %Collector.SnapshotRow{
        map_id: 49,
        x: -152,
        y: 200,
        tribe: 1,
        village_id: "p2",
        village_server_id: 19702,
        village_name: "a2",
        player_id: "p1",
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
      %Collector.SnapshotRow{
        map_id: 30,
        x: -151,
        y: 180,
        tribe: 3,
        village_id: "p3",
        village_server_id: 19996,
        village_name: "a3",
        player_id: "p1",
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

    prev_player_snapshot = [
      %Collector.SnapshotRow{
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
      %Collector.SnapshotRow{
        map_id: 49,
        x: -152,
        y: 200,
        tribe: 1,
        village_id: "p2",
        village_server_id: 19702,
        village_name: "a2",
        player_id: "p1",
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
      }
    ]

    inc =
      Collector.AggPlayers.Increment.increment(
        target_dt,
        player_id,
        new_player_snapshot,
        prev_player_snapshot
      )

    assert(is_struct(inc, Collector.AggPlayers.Increment))
    assert(inc.target_dt == target_dt)
    assert(inc.population_increase_by_founded == 1200)
  end

  test "AggPlayers.increment() defines population_increase_by_conquered as the sum of conquered village's populations in new snapshot" do
    target_dt = DateTime.utc_now()
    player_id = "p1"

    new_player_snapshot = [
      %Collector.SnapshotRow{
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
      %Collector.SnapshotRow{
        map_id: 49,
        x: -152,
        y: 200,
        tribe: 1,
        village_id: "p2",
        village_server_id: 19702,
        village_name: "a2",
        player_id: "p1",
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
      %Collector.SnapshotRow{
        map_id: 30,
        x: -151,
        y: 180,
        tribe: 3,
        village_id: "p3",
        village_server_id: 19996,
        village_name: "a3",
        player_id: "p1",
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

    prev_player_snapshot = [
      %Collector.SnapshotRow{
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
      %Collector.SnapshotRow{
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
      %Collector.SnapshotRow{
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
        alliance_id: "a2",
        alliance_server_id: 8,
        alliance_name: "alskj",
        population: 1200,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      }
    ]

    inc =
      Collector.AggPlayers.Increment.increment(
        target_dt,
        player_id,
        new_player_snapshot,
        prev_player_snapshot
      )

    assert(is_struct(inc, Collector.AggPlayers.Increment))
    assert(inc.target_dt == target_dt)
    assert(inc.population_increase_by_conquered == 2164)
  end

  test "AggPlayers.increment() defines population_decrease as the sum of village's populations increment if this increment is negative, it is an absolute value" do
    target_dt = DateTime.utc_now()
    player_id = "p1"

    new_player_snapshot = [
      %Collector.SnapshotRow{
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
        population: 900,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      },
      %Collector.SnapshotRow{
        map_id: 49,
        x: -152,
        y: 200,
        tribe: 1,
        village_id: "p2",
        village_server_id: 19702,
        village_name: "a2",
        player_id: "p1",
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
      %Collector.SnapshotRow{
        map_id: 30,
        x: -151,
        y: 180,
        tribe: 3,
        village_id: "p3",
        village_server_id: 19996,
        village_name: "a3",
        player_id: "p1",
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

    prev_player_snapshot = [
      %Collector.SnapshotRow{
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
      %Collector.SnapshotRow{
        map_id: 49,
        x: -152,
        y: 200,
        tribe: 1,
        village_id: "p2",
        village_server_id: 19702,
        village_name: "a2",
        player_id: "p1",
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
      %Collector.SnapshotRow{
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
        alliance_id: "a2",
        alliance_server_id: 8,
        alliance_name: "alskj",
        population: 1200,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      }
    ]

    inc =
      Collector.AggPlayers.Increment.increment(
        target_dt,
        player_id,
        new_player_snapshot,
        prev_player_snapshot
      )

    assert(is_struct(inc, Collector.AggPlayers.Increment))
    assert(inc.target_dt == target_dt)
    assert(inc.population_decrease == 61)
  end

  test "AggPlayers.increment() defines population_decrease_by_conquered as the sum of lost_conquered village's populations in new snapshot" do
    target_dt = DateTime.utc_now()
    player_id = "p1"

    new_player_snapshot = [
      %Collector.SnapshotRow{
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
      %Collector.SnapshotRow{
        map_id: 49,
        x: -152,
        y: 200,
        tribe: 1,
        village_id: "p2",
        village_server_id: 19702,
        village_name: "a2",
        player_id: "p3",
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
      %Collector.SnapshotRow{
        map_id: 30,
        x: -151,
        y: 180,
        tribe: 3,
        village_id: "p3",
        village_server_id: 19996,
        village_name: "a3",
        player_id: "p1",
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

    prev_player_snapshot = [
      %Collector.SnapshotRow{
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
      %Collector.SnapshotRow{
        map_id: 49,
        x: -152,
        y: 200,
        tribe: 1,
        village_id: "p2",
        village_server_id: 19702,
        village_name: "a2",
        player_id: "p1",
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
      %Collector.SnapshotRow{
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
        alliance_id: "a2",
        alliance_server_id: 8,
        alliance_name: "alskj",
        population: 1200,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      }
    ]

    inc =
      Collector.AggPlayers.Increment.increment(
        target_dt,
        player_id,
        new_player_snapshot,
        prev_player_snapshot
      )

    assert(is_struct(inc, Collector.AggPlayers.Increment))
    assert(inc.target_dt == target_dt)
    assert(inc.population_decrease_by_conquered == 964)
  end

  test "AggPlayers.increment() defines population_decrease_by_destroyed as the sum of destroyed village's populations in previous snapshot" do
    target_dt = DateTime.utc_now()
    player_id = "p1"

    new_player_snapshot = [
      %Collector.SnapshotRow{
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
      %Collector.SnapshotRow{
        map_id: 30,
        x: -151,
        y: 180,
        tribe: 3,
        village_id: "p3",
        village_server_id: 19996,
        village_name: "a3",
        player_id: "p1",
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

    prev_player_snapshot = [
      %Collector.SnapshotRow{
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
      %Collector.SnapshotRow{
        map_id: 49,
        x: -152,
        y: 200,
        tribe: 1,
        village_id: "p2",
        village_server_id: 19702,
        village_name: "a2",
        player_id: "p1",
        player_server_id: 416,
        player_name: "opc",
        alliance_id: "a1",
        alliance_server_id: 8,
        alliance_name: "WW",
        population: 950,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      },
      %Collector.SnapshotRow{
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
        alliance_id: "a2",
        alliance_server_id: 8,
        alliance_name: "alskj",
        population: 1200,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      }
    ]

    inc =
      Collector.AggPlayers.Increment.increment(
        target_dt,
        player_id,
        new_player_snapshot,
        prev_player_snapshot
      )

    assert(is_struct(inc, Collector.AggPlayers.Increment))
    assert(inc.target_dt == target_dt)
    assert(inc.population_decrease_by_destroyed == 950)
  end

  test "AggPlayers.increment() defines n_villages_with_population_increase as the count of villages whos population increment is positive" do
    target_dt = DateTime.utc_now()
    player_id = "p1"

    new_player_snapshot = [
      %Collector.SnapshotRow{
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
        population: 1000,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      },
      %Collector.SnapshotRow{
        map_id: 49,
        x: -152,
        y: 200,
        tribe: 1,
        village_id: "p2",
        village_server_id: 19702,
        village_name: "a2",
        player_id: "p1",
        player_server_id: 416,
        player_name: "opc",
        alliance_id: "a1",
        alliance_server_id: 8,
        alliance_name: "WW",
        population: 1000,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      },
      %Collector.SnapshotRow{
        map_id: 30,
        x: -151,
        y: 180,
        tribe: 3,
        village_id: "p3",
        village_server_id: 19996,
        village_name: "a3",
        player_id: "p1",
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

    prev_player_snapshot = [
      %Collector.SnapshotRow{
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
      %Collector.SnapshotRow{
        map_id: 49,
        x: -152,
        y: 200,
        tribe: 1,
        village_id: "p2",
        village_server_id: 19702,
        village_name: "a2",
        player_id: "p1",
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
      %Collector.SnapshotRow{
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
        alliance_id: "a2",
        alliance_server_id: 8,
        alliance_name: "alskj",
        population: 1200,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      }
    ]

    inc =
      Collector.AggPlayers.Increment.increment(
        target_dt,
        player_id,
        new_player_snapshot,
        prev_player_snapshot
      )

    assert(is_struct(inc, Collector.AggPlayers.Increment))
    assert(inc.target_dt == target_dt)
    assert(inc.n_villages_with_population_increase == 2)
  end

  test "AggPlayers.increment() defines n_villages_with_population_decrease as the count of villages whos population increment is negative" do
    target_dt = DateTime.utc_now()
    player_id = "p1"

    new_player_snapshot = [
      %Collector.SnapshotRow{
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
        population: 100,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      },
      %Collector.SnapshotRow{
        map_id: 49,
        x: -152,
        y: 200,
        tribe: 1,
        village_id: "p2",
        village_server_id: 19702,
        village_name: "a2",
        player_id: "p1",
        player_server_id: 416,
        player_name: "opc",
        alliance_id: "a1",
        alliance_server_id: 8,
        alliance_name: "WW",
        population: 1000,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      },
      %Collector.SnapshotRow{
        map_id: 30,
        x: -151,
        y: 180,
        tribe: 3,
        village_id: "p3",
        village_server_id: 19996,
        village_name: "a3",
        player_id: "p1",
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

    prev_player_snapshot = [
      %Collector.SnapshotRow{
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
      %Collector.SnapshotRow{
        map_id: 49,
        x: -152,
        y: 200,
        tribe: 1,
        village_id: "p2",
        village_server_id: 19702,
        village_name: "a2",
        player_id: "p1",
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
      %Collector.SnapshotRow{
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
        alliance_id: "a2",
        alliance_server_id: 8,
        alliance_name: "alskj",
        population: 1200,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      }
    ]

    inc =
      Collector.AggPlayers.Increment.increment(
        target_dt,
        player_id,
        new_player_snapshot,
        prev_player_snapshot
      )

    assert(is_struct(inc, Collector.AggPlayers.Increment))
    assert(inc.target_dt == target_dt)
    assert(inc.n_villages_with_population_decrease == 1)
  end

  test "AggPlayers.increment() defines n_villages_with_population_stuck as the count of villages whos population increment is 0" do
    target_dt = DateTime.utc_now()
    player_id = "p1"

    new_player_snapshot = [
      %Collector.SnapshotRow{
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
      %Collector.SnapshotRow{
        map_id: 49,
        x: -152,
        y: 200,
        tribe: 1,
        village_id: "p2",
        village_server_id: 19702,
        village_name: "a2",
        player_id: "p1",
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
      %Collector.SnapshotRow{
        map_id: 30,
        x: -151,
        y: 180,
        tribe: 3,
        village_id: "p3",
        village_server_id: 19996,
        village_name: "a3",
        player_id: "p1",
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

    prev_player_snapshot = [
      %Collector.SnapshotRow{
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
      %Collector.SnapshotRow{
        map_id: 49,
        x: -152,
        y: 200,
        tribe: 1,
        village_id: "p2",
        village_server_id: 19702,
        village_name: "a2",
        player_id: "p1",
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
      %Collector.SnapshotRow{
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
        alliance_id: "a2",
        alliance_server_id: 8,
        alliance_name: "alskj",
        population: 1200,
        region: nil,
        is_capital: false,
        is_city: nil,
        victory_points: nil
      }
    ]

    inc =
      Collector.AggPlayers.Increment.increment(
        target_dt,
        player_id,
        new_player_snapshot,
        prev_player_snapshot
      )

    assert(is_struct(inc, Collector.AggPlayers.Increment))
    assert(inc.target_dt == target_dt)
    assert(inc.n_villages_with_population_stuck == 2)
  end

  test "If there is no previous snapshot, AggPlayers.increment() defines increment fields as nil" do
    target_dt = DateTime.utc_now()

    new_player_snapshot = [
      %Collector.SnapshotRow{
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
      %Collector.SnapshotRow{
        map_id: 49,
        x: -152,
        y: 200,
        tribe: 1,
        village_id: "p2",
        village_server_id: 19702,
        village_name: "a2",
        player_id: "p1",
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
      %Collector.SnapshotRow{
        map_id: 30,
        x: -151,
        y: 180,
        tribe: 3,
        village_id: "p3",
        village_server_id: 19996,
        village_name: "a3",
        player_id: "p1",
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

    expected_output = %Collector.AggPlayers.Increment{
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

    inc = Collector.AggPlayers.Increment.increment(target_dt, new_player_snapshot)
    assert(expected_output == inc)
  end
end
