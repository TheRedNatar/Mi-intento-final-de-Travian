defmodule Collector.RawSnapshotTest do
  use ExUnit.Case

  @tag :tmp_dir
  test "RawSnapshot.run() fetch a map.sql file from a Travian server", %{tmp_dir: root_folder} do
    target_date = Date.utc_today()
    server_id = "https://ts4.x1.america.travian.com"

    assert(:ok == Collector.RawSnapshot.run(root_folder, server_id, target_date))

    {:ok, raw_snapshot} =
      Collector.Feed.open(root_folder, server_id, target_date, Collector.RawSnapshot)

    assert(is_bitstring(raw_snapshot))
  end

  @tag :tmp_dir
  test "RawSnapshot.run() returns error if the server does not exist", %{tmp_dir: root_folder} do
    target_date = Date.utc_today()
    server_id = "bad_server"

    {:error, {error_msg, _reason}} =
      Collector.RawSnapshot.run(root_folder, server_id, target_date)

    assert(error_msg == "Unable to fetch server from Travian")
  end
end
