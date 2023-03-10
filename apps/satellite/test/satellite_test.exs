defmodule SatelliteTest do
  use ExUnit.Case
  doctest Satellite

  setup do
    target_date = Date.utc_today()
    creation_dt = DateTime.utc_now()
    server_id = "https://czsk.x1.czsk.travian.com"

    medusa_rows = [
      %Satellite.MedusaTable{
        alliance_id: "https://czsk.x1.czsk.travian.com--A--18",
        server_id: server_id,
        server_url: server_id,
        alliance_name: "00A",
        alliance_url: "00A",
        inactive_in_current: :undefined,
        inactive_in_future: true,
        inactive_probability: 0.8,
        model: :player_1,
        n_villages: 1,
        player_id: "https://czsk.x1.czsk.travian.com--P--1009",
        player_name: "PalMer",
        player_url: "https://czsk.x1.czsk.travian.com--P--1009",
        center_mass_x: 4,
        center_mass_y: -10,
        target_date: target_date,
        creation_dt: creation_dt,
        total_population: 227
      },
      %Satellite.MedusaTable{
        alliance_id: "https://czsk.x1.czsk.travian.com--A--26",
        server_id: server_id,
        server_url: server_id,
        alliance_name: "ZBL",
        alliance_url: "ZBL",
        inactive_in_current: :undefined,
        inactive_in_future: true,
        inactive_probability: 0.3,
        model: :player_1,
        n_villages: 2,
        player_id: "https://czsk.x1.czsk.travian.com--P--815",
        player_name: "Klobuk",
        player_url: "https://czsk.x1.czsk.travian.com--P--815",
        center_mass_x: 29,
        center_mass_y: 80,
        target_date: target_date,
        creation_dt: creation_dt,
        total_population: 1469
      }
    ]

    %{medusa_rows: medusa_rows, server_id: server_id}
  end

  @tag :tmp_dir
  test "MedusaTable.insert_predictions inserts medusa_rows in medusa_table", %{
    medusa_rows: mr,
    server_id: server_id,
    tmp_dir: mnesia_dir
  } do
    install(mnesia_dir)
    assert([:ok, :ok] == Satellite.MedusaTable.insert_predictions(mr))
    output = Satellite.MedusaTable.get_predictions_by_server(server_id)

    for x <- mr, do: assert(x in output)
  end

  @tag :tmp_dir
  test "MedusaTable.get_predictions_by_server fetch only rows based on target_date with Date.utc_today as default",
       %{
         medusa_rows: [one, two],
         server_id: server_id,
         tmp_dir: mnesia_dir
       } do
    install(mnesia_dir)
    today = Date.utc_today()
    yesterday = Date.add(today, -1)
    yesterday_row = Map.put(two, :target_date, yesterday)

    assert([:ok, :ok] == Satellite.MedusaTable.insert_predictions([one, yesterday_row]))
    assert([one] == Satellite.MedusaTable.get_predictions_by_server(server_id))

    assert(
      [yesterday_row] == Satellite.MedusaTable.get_predictions_by_server(server_id, yesterday)
    )
  end

  @tag :tmp_dir
  test "ServersTable.upsert_server! upserts servers in ServersTable", %{
    server_id: server_id,
    tmp_dir: mnesia_dir
  } do
    today = Date.utc_today()
    yesterday = Date.add(today, -1)
    install(mnesia_dir)

    assert(:ok == Satellite.ServersTable.upsert_server!(server_id))
    assert([server_id] == Satellite.ServersTable.get_servers!())
    assert([server_id] == Satellite.ServersTable.get_servers!(today))
    assert([] == Satellite.ServersTable.get_servers!(yesterday))
  end

  defp install(mnesia_dir) do
    dir = File.mkdir_p!(mnesia_dir <> "/mnesia_dir")
    Application.put_env(:mnesia, :dir, dir)
    assert(:ok == Satellite.install([Node.self()]))
  end
end
