defmodule Collector.SnapshotTest do
  use ExUnit.Case

  test "Snapshot.process_rows() transform the raw_snapshot rows to a list of Snapshot structs and creates the *_id fields" do
    server_id = "server_x"

    raw_snapshot =
      "INSERT INTO `x_world` VALUES (27,-174,200,3,39983,'New village',13,'Masbro',251,'ÖFKE',41,NULL,FALSE,NULL,NULL);
INSERT INTO `x_world` VALUES (57,-144,200,6,39161,'05',374,'LosDosHermanos',210,'Hags',225,NULL,FALSE,NULL,NULL);
INSERT INTO `x_world` VALUES (58,-143,200,6,32245,'02',374,'LosDosHermanos',210,'Hags',721,NULL,FALSE,NULL,NULL);
INSERT INTO `x_world` VALUES (82,-119,200,6,39221,'01 Fonseca',2265,'Fonseca',250,'W.S0',90,NULL,FALSE,NULL,NULL);
INSERT INTO `x_world` VALUES (115,-86,200,1,39368,'02',9808,'Aeirdun',235,'AP',104,NULL,FALSE,NULL,NULL);"

    {output_rows, error_rows} = Collector.Snapshot.process_rows(raw_snapshot, server_id)

    expected_rows = [
      %Collector.Snapshot{
        alliance_id: "server_x--A--251",
        alliance_name: "ÖFKE",
        alliance_server_id: 251,
        is_capital: false,
        is_city: nil,
        map_id: 27,
        player_id: "server_x--P--13",
        player_name: "Masbro",
        player_server_id: 13,
        population: 41,
        region: nil,
        tribe: 3,
        victory_points: nil,
        village_id: "server_x--V--39983",
        village_name: "New village",
        village_server_id: 39983,
        x: -174,
        y: 200
      },
      %Collector.Snapshot{
        alliance_id: "server_x--A--210",
        alliance_name: "Hags",
        alliance_server_id: 210,
        is_capital: false,
        is_city: nil,
        map_id: 57,
        player_id: "server_x--P--374",
        player_name: "LosDosHermanos",
        player_server_id: 374,
        population: 225,
        region: nil,
        tribe: 6,
        victory_points: nil,
        village_id: "server_x--V--39161",
        village_name: "05",
        village_server_id: 39161,
        x: -144,
        y: 200
      },
      %Collector.Snapshot{
        alliance_id: "server_x--A--210",
        alliance_name: "Hags",
        alliance_server_id: 210,
        is_capital: false,
        is_city: nil,
        map_id: 58,
        player_id: "server_x--P--374",
        player_name: "LosDosHermanos",
        player_server_id: 374,
        population: 721,
        region: nil,
        tribe: 6,
        victory_points: nil,
        village_id: "server_x--V--32245",
        village_name: "02",
        village_server_id: 32245,
        x: -143,
        y: 200
      },
      %Collector.Snapshot{
        alliance_id: "server_x--A--250",
        alliance_name: "W.S0",
        alliance_server_id: 250,
        is_capital: false,
        is_city: nil,
        map_id: 82,
        player_id: "server_x--P--2265",
        player_name: "Fonseca",
        player_server_id: 2265,
        population: 90,
        region: nil,
        tribe: 6,
        victory_points: nil,
        village_id: "server_x--V--39221",
        village_name: "01 Fonseca",
        village_server_id: 39221,
        x: -119,
        y: 200
      },
      %Collector.Snapshot{
        alliance_id: "server_x--A--235",
        alliance_name: "AP",
        alliance_server_id: 235,
        is_capital: false,
        is_city: nil,
        map_id: 115,
        player_id: "server_x--P--9808",
        player_name: "Aeirdun",
        player_server_id: 9808,
        population: 104,
        region: nil,
        tribe: 1,
        victory_points: nil,
        village_id: "server_x--V--39368",
        village_name: "02",
        village_server_id: 39368,
        x: -86,
        y: 200
      }
    ]

    assert(error_rows == [])
    assert(length(output_rows) == length(expected_rows))
    for x <- expected_rows, do: assert(x in output_rows)
  end

  test "Snapshot.process_rows() catch error rows" do
    server_id = "server_x"

    raw_snapshot =
      "INSERT INTO `x_world` VALUES (27,-174,200,3,39983,'New village',13,'Masbro',251,'ÖFKE');
INSERT INTO `x_world` VALUES (115,-86,200,1,39368,'02',9808,'Aeirdun',235,'AP',104,NULL,FALSE,NULL,NULL);"

    {output_rows, error_rows} = Collector.Snapshot.process_rows(raw_snapshot, server_id)

    expected_rows = [
      %Collector.Snapshot{
        alliance_id: "server_x--A--235",
        alliance_name: "AP",
        alliance_server_id: 235,
        is_capital: false,
        is_city: nil,
        map_id: 115,
        player_id: "server_x--P--9808",
        player_name: "Aeirdun",
        player_server_id: 9808,
        population: 104,
        region: nil,
        tribe: 1,
        victory_points: nil,
        village_id: "server_x--V--39368",
        village_name: "02",
        village_server_id: 39368,
        x: -86,
        y: 200
      }
    ]

    assert(error_rows == [{:error, 'broken line, not enought comas'}])
    assert(length(output_rows) == length(expected_rows))
    for x <- expected_rows, do: assert(x in output_rows)
  end

  @tag :tmp_dir
  test "Snapshot.run() returns unable to open the file if there is no raw_snapshot of target_date",
       %{tmp_dir: root_folder} do
    target_date = Date.utc_today()
    server_id = "server_x"

    {atom_error, {string_raw_snapshot_open, _}} =
      Collector.Snapshot.run(root_folder, server_id, target_date)

    assert(atom_error == :error)
    assert(string_raw_snapshot_open == "Unable to open raw_snapshot")
  end

  @tag :tmp_dir
  test "Snapshot.run() creates a snapshot table of target_date using the raw_snapshot of target_date",
       %{tmp_dir: root_folder} do
    raw_snapshot =
      "INSERT INTO `x_world` VALUES (27,-174,200,3,39983,'New village',13,'Masbro',251,'ÖFKE',41,NULL,FALSE,NULL,NULL);
INSERT INTO `x_world` VALUES (57,-144,200,6,39161,'05',374,'LosDosHermanos',210,'Hags',225,NULL,FALSE,NULL,NULL);
INSERT INTO `x_world` VALUES (58,-143,200,6,32245,'02',374,'LosDosHermanos',210,'Hags',721,NULL,FALSE,NULL,NULL);
INSERT INTO `x_world` VALUES (82,-119,200,6,39221,'01 Fonseca',2265,'Fonseca',250,'W.S0',90,NULL,FALSE,NULL,NULL);
INSERT INTO `x_world` VALUES (115,-86,200,1,39368,'02',9808,'Aeirdun',235,'AP',104,NULL,FALSE,NULL,NULL);"

    target_date = Date.utc_today()
    server_id = "server_x"

    assert(
      :ok ==
        Collector.Feed.store(
          root_folder,
          server_id,
          target_date,
          raw_snapshot,
          Collector.RawSnapshot
        )
    )

    assert(:ok == Collector.Snapshot.run(root_folder, server_id, target_date))
    {:ok, snapshot} = Collector.Feed.open(root_folder, server_id, target_date, Collector.Snapshot)

    for x <- snapshot, do: assert(is_struct(x, Collector.Snapshot))
  end

  @tag :tmp_dir
  test "Snapshot.run() creates a snapshot_errors table of target_date in case raw_snapshot of target_date has malformed rows",
       %{tmp_dir: root_folder} do
    raw_snapshot =
      "INSERT INTO `x_world` VALUES (27,-174,200,3,39983,'New village',13,'Masbro',251,'ÖFKE');
INSERT INTO `x_world` VALUES (115,-86,200,1,39368,'02',9808,'Aeirdun',235,'AP',104,NULL,FALSE,NULL,NULL);"

    target_date = Date.utc_today()
    server_id = "server_x"

    assert(
      :ok ==
        Collector.Feed.store(
          root_folder,
          server_id,
          target_date,
          raw_snapshot,
          Collector.RawSnapshot
        )
    )

    assert(:ok == Collector.Snapshot.run(root_folder, server_id, target_date))
    {:ok, snapshot} = Collector.Feed.open(root_folder, server_id, target_date, Collector.Snapshot)
    {:ok, snapshot_errors} = Collector.Snapshot.open_errors(root_folder, server_id, target_date)

    assert(length(snapshot) == 1)
    assert(snapshot_errors != [])
  end
end
