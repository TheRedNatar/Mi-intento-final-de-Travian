defmodule CollectorArchTest do
  use ExUnit.Case

  setup_all do
    %{server_id: "https://ts5.x1.europe.travian.com"}
  end

  @tag :skip
  test "Collector.suscribe() makes you recieve events from collection" do
  end

  @tag :tmp_dir
  test "Collector.Supervisor.Worker.start_child launch a GenWorker which runs a DAG", %{
    tmp_dir: root_folder,
    server_id: server_id
  } do
    Application.put_env(:collector, :min, 1_000)
    Application.put_env(:collector, :max, 2_000)
    target_date = Date.utc_today()
    assert(Supervisor.count_children(Collector.Supervisor.Worker)[:workers] == 0)

    {:ok, {pid, ref}} =
      Collector.Supervisor.Worker.start_child(root_folder, server_id, target_date)

    assert(Supervisor.count_children(Collector.Supervisor.Worker)[:workers] == 1)
    assert_receive({:DOWN, ^ref, :process, ^pid, :normal}, 5_000)
    assert(Storage.exist?(root_folder, server_id, Collector.RawSnapshot.options(), target_date))

    assert(
      Storage.exist?(root_folder, server_id, Collector.Snapshot.snapshot_options(), target_date)
    )

    assert(Storage.exist?(root_folder, server_id, Collector.AggPlayers.options(), target_date))
  end

  @tag :tmp_dir
  test "Collector.GenWorker runs the whole DAG for server_id", %{
    tmp_dir: root_folder,
    server_id: server_id
  } do
    target_date = Date.utc_today()
    attemps = 1
    min = 1_000
    max = 2_000

    child_spec = %{
      id: "Collector.GenWorker",
      start:
        {Collector.GenWorker, :start_link,
         [root_folder, server_id, target_date, attemps, min, max]},
      restart: :transient
    }

    pid = start_supervised!(child_spec)
    ref = Process.monitor(pid)
    assert_receive({:DOWN, ^ref, :process, ^pid, :normal}, 5_000)
    assert(Storage.exist?(root_folder, server_id, Collector.RawSnapshot.options(), target_date))

    assert(
      Storage.exist?(root_folder, server_id, Collector.Snapshot.snapshot_options(), target_date)
    )

    assert(Storage.exist?(root_folder, server_id, Collector.AggPlayers.options(), target_date))
  end

  @tag :tmp_dir
  test "Collector.GenWorker runs and do nothing with a bad_server_id but it doesn't fail", %{
    tmp_dir: root_folder
  } do
    target_date = Date.utc_today()
    server_id = "https://no_exists.travian.com"
    attemps = 1
    min = 1_000
    max = 2_000

    child_spec = %{
      id: "Collector.GenWorker",
      start:
        {Collector.GenWorker, :start_link,
         [root_folder, server_id, target_date, attemps, min, max]},
      restart: :transient
    }

    pid = start_supervised!(child_spec)
    ref = Process.monitor(pid)
    assert_receive({:DOWN, ^ref, :process, ^pid, :normal}, 5_000)
    assert(!Storage.exist?(root_folder, server_id, Collector.RawSnapshot.options(), target_date))

    assert(
      !Storage.exist?(root_folder, server_id, Collector.Snapshot.snapshot_options(), target_date)
    )

    assert(!Storage.exist?(root_folder, server_id, Collector.AggPlayers.options(), target_date))
  end
end
