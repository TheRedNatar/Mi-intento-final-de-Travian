defmodule Collector.AggServerTest do
  use ExUnit.Case

  setup_all do
    %{
      server_id: "ts6.x1.europe.travian.com",
      target_date: Date.utc_today(),
      snapshot: File.read!("test/resources/snapshot_sample_with_regions.c6bert") |> Collector.snapshot_from_format()
    }
  end

  @tag tmp_dir: true
  test "Collector.AggServer.run with no previous AggServer", %{server_id: server_id, target_date: target_date, snapshot: snapshot, tmp_dir: root_folder} do
    :ok = Storage.store(root_folder, server_id, Collector.snapshot_options(), Collector.snapshot_to_format(snapshot), target_date)

    assert(:ok == Collector.AggServer.run(root_folder, server_id, target_date))
    {:ok, {_, encoded_agg_server}} = Storage.open(root_folder, server_id, Collector.agg_server_options(), target_date)
    output = Collector.agg_server_from_format(encoded_agg_server)

    assert(is_struct(output, Collector.AggServer))

    assert(output.target_date == target_date)
    assert(is_struct(output.extraction_date, DateTime))
    assert(output.server_id == server_id)
    assert(output.server_url == server_id)
    assert(output.server_contraction == "ts6")
    assert(output.server_speed == 1)
    assert(output.server_region == "europe")
    assert(output.estimated_starting_date == target_date)

    # Villages
    assert(length(output.villages) == length(snapshot))
    for r <- snapshot, do: r.village_id in output.villages
    assert(output.new_villages == nil)
    assert(output.removed_villages == nil)

    # Players
    assert(length(output.players) == Enum.uniq_by(snapshot, fn x -> x.player_id end) |> length())
    for r <- snapshot, do: r.player_id in output.players
    assert(output.new_players == nil)
    assert(output.removed_players == nil)

    # Alliances
    assert(length(output.alliances) == Enum.uniq_by(snapshot, fn x -> x.alliance_id end) |> length())
    for r <- snapshot, do: r.alliance_id in output.alliances
    assert(output.new_alliances == nil)
    assert(output.removed_alliances == nil)
  end


  @tag tmp_dir: true
  test "Collector.AggServer.run with previous AggServer being the first time", %{server_id: server_id, target_date: target_date, snapshot: snapshot, tmp_dir: root_folder} do

    [old_first | _] = snapshot

    new_old_row = old_first
    |> Map.put(:village_id, "old_village")
    |> Map.put(:player_id, "old_player")
    |> Map.put(:alliance_id, "old_alliance")

    old_snapshot = snapshot ++ [new_old_row]


    :ok = Storage.store(root_folder, server_id, Collector.snapshot_options(), Collector.snapshot_to_format(old_snapshot), target_date)

    tomorrow = Date.add(target_date, 1)
    {poped, snapshot_tomorrow} = Enum.split(snapshot, 2)
    [first, _second] = poped

    new_row = first
    |> Map.put(:village_id, "new_village")
    |> Map.put(:player_id, "new_player")
    |> Map.put(:alliance_id, "new_alliance")

    snapshot_tomorrow = snapshot_tomorrow ++ [new_row]
    :ok = Storage.store(root_folder, server_id, Collector.snapshot_options(), Collector.snapshot_to_format(snapshot_tomorrow), tomorrow)


    assert(:ok == Collector.AggServer.run(root_folder, server_id, target_date))
    assert(:ok == Collector.AggServer.run(root_folder, server_id, tomorrow))

    {:ok, {_, encoded_agg_server}} = Storage.open(root_folder, server_id, Collector.agg_server_options(), tomorrow)
    output = Collector.agg_server_from_format(encoded_agg_server)

    assert(is_struct(output, Collector.AggServer))

    assert(output.target_date == tomorrow)
    assert(is_struct(output.extraction_date, DateTime))
    assert(output.server_id == server_id)
    assert(output.server_url == server_id)
    assert(output.server_contraction == "ts6")
    assert(output.server_speed == 1)
    assert(output.server_region == "europe")
    assert(output.estimated_starting_date == target_date)

    # Villages
    assert(length(output.villages) == length(snapshot_tomorrow))
    for r <- snapshot_tomorrow, do: r.village_id in output.villages
    assert(output.new_villages == ["new_village"])
    removed_villages = Enum.map(poped, &(&1.village_id)) ++ ["old_village"]
    assert(Enum.sort(output.removed_villages) ==  Enum.sort(removed_villages))

    # Players
    assert(length(output.players) == Enum.uniq_by(snapshot_tomorrow, fn x -> x.player_id end) |> length())
    for r <- snapshot_tomorrow, do: r.player_id in output.players
    assert(output.new_players == ["new_player"])
    assert(Enum.sort(output.removed_players) == ["old_player"])

    # Alliances
    assert(length(output.alliances) == Enum.uniq_by(snapshot_tomorrow, fn x -> x.alliance_id end) |> length())
    for r <- snapshot_tomorrow, do: r.alliance_id in output.alliances
    assert(output.new_alliances == ["new_alliance"])
    assert(Enum.sort(output.removed_alliances) == ["old_alliance"])
  end


  @tag tmp_dir: true
  test "Collector.AggServer.run with previous AggServer", %{server_id: server_id, target_date: target_date, snapshot: snapshot, tmp_dir: root_folder} do

    tomorrow1 = Date.add(target_date, 1)
    tomorrow2 = Date.add(target_date, 2)


    [old_first | _] = snapshot

    new_old_row = old_first
    |> Map.put(:village_id, "old_village")
    |> Map.put(:player_id, "old_player")
    |> Map.put(:alliance_id, "old_alliance")

    snapshot_tomorrow1 = snapshot ++ [new_old_row]


    {poped, snapshot_tomorrow2} = Enum.split(snapshot, 2)
    [first, _second] = poped

    new_row = first
    |> Map.put(:village_id, "new_village")
    |> Map.put(:player_id, "new_player")
    |> Map.put(:alliance_id, "new_alliance")

    snapshot_tomorrow2 = snapshot_tomorrow2 ++ [new_row]

    :ok = Storage.store(root_folder, server_id, Collector.snapshot_options(), Collector.snapshot_to_format(snapshot), target_date)
    :ok = Storage.store(root_folder, server_id, Collector.snapshot_options(), Collector.snapshot_to_format(snapshot_tomorrow1), tomorrow1)
    :ok = Storage.store(root_folder, server_id, Collector.snapshot_options(), Collector.snapshot_to_format(snapshot_tomorrow2), tomorrow2)


    assert(:ok == Collector.AggServer.run(root_folder, server_id, target_date))
    assert(:ok == Collector.AggServer.run(root_folder, server_id, tomorrow1))
    assert(:ok == Collector.AggServer.run(root_folder, server_id, tomorrow2))

    {:ok, {_, encoded_agg_server}} = Storage.open(root_folder, server_id, Collector.agg_server_options(), tomorrow2)
    output = Collector.agg_server_from_format(encoded_agg_server)

    assert(is_struct(output, Collector.AggServer))

    assert(output.target_date == tomorrow2)
    assert(is_struct(output.extraction_date, DateTime))
    assert(output.server_id == server_id)
    assert(output.server_url == server_id)
    assert(output.server_contraction == "ts6")
    assert(output.server_speed == 1)
    assert(output.server_region == "europe")
    assert(output.estimated_starting_date == target_date)

    # Villages
    assert(length(output.villages) == length(snapshot_tomorrow2))
    for r <- snapshot_tomorrow2, do: r.village_id in output.villages
    assert(output.new_villages == ["new_village"])
    removed_villages = Enum.map(poped, &(&1.village_id)) ++ ["old_village"]
    assert(Enum.sort(output.removed_villages) ==  Enum.sort(removed_villages))

    # Players
    assert(length(output.players) == Enum.uniq_by(snapshot_tomorrow2, fn x -> x.player_id end) |> length())
    for r <- snapshot_tomorrow2, do: r.player_id in output.players
    assert(output.new_players == ["new_player"])
    assert(Enum.sort(output.removed_players) == ["old_player"])

    # Alliances
    assert(length(output.alliances) == Enum.uniq_by(snapshot_tomorrow2, fn x -> x.alliance_id end) |> length())
    for r <- snapshot_tomorrow2, do: r.alliance_id in output.alliances
    assert(output.new_alliances == ["new_alliance"])
    assert(Enum.sort(output.removed_alliances) == ["old_alliance"])
  end


  test "Collector.AggServer.process with server_id not splitable put nil on contraction, speed and region", %{target_date: target_date, snapshot: snapshot} do
    bad_server_id = "no splitable server id"
    agg_server_id = Collector.AggServer.process(bad_server_id, target_date, snapshot, nil)

    assert(agg_server_id.server_contraction == nil)
    assert(agg_server_id.server_speed == nil)
    assert(agg_server_id.server_region == nil)
  end
end
