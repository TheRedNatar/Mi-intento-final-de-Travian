defmodule CollectorArchTest do
  use ExUnit.Case

  setup_all do
    %{server_id: "https://ts5.x1.europe.travian.com"}
  end

  # Event tests
  @tag :tmp_dir
  test "If you are subscribed and the collection starts, you will received and start_event", %{
    tmp_dir: root_folder
  } do

    # We don't want to start the collection, just the event
    Application.put_env(:collector, :min, 200_000)
    Application.put_env(:collector, :max, 300_000)
    
    Application.put_env(:collector, :root_folder, root_folder)
    
    assert(is_reference(Collector.subscribe()))
    assert(:ok = Collector.collect())
    assert_receive({:collector_event, :collection_started}, 5_000)
    Application.stop(:collector)
    Application.start(:collector)
  end


  test "If you are subscribed and the collection ends, you will received and end_event", %{
    server_id: server_id
    } do
    pid = :c.pid(0, 250, 0)
    ref = make_ref()

    target_date = Date.utc_today()
    subscriptions = [self()]
    active_p = %{pid => {ref, server_id}}
    state = %Collector.GenCollector{target_date: target_date, subscriptions: subscriptions, active_p: active_p}

    {:noreply, new_state, {:continue, :is_finished}} = Collector.GenCollector.handle_cast({{:error, "some_error"}, server_id, pid}, state)
    assert(new_state.active_p == %{})
    Collector.GenCollector.handle_continue(:is_finished, new_state)
    assert_receive({:collector_event, :collection_finished}, 5_000)
  end


  test "If you are subscribed and the collection of a server_id finish, you will received and server_id end event", %{
    server_id: server_id
  } do

    pid = :c.pid(0, 250, 0)
    ref = make_ref()

    target_date = Date.utc_today()
    subscriptions = [self()]
    active_p = %{pid => {ref, server_id}}
    state = %Collector.GenCollector{target_date: target_date, subscriptions: subscriptions, active_p: active_p}

    Collector.GenCollector.handle_cast({:ok, server_id, pid}, state)
    assert_receive({:collector_event, {:ok, ^server_id}}, 5_000)
    Collector.GenCollector.handle_cast({{:error, "some_error"}, server_id, pid}, state)
    assert_receive({:collector_event, {{:error, "some_error"}, ^server_id}}, 5_000)
  end

  # Server_Id process behaviours
  test "If a server_id process finish its collection ok or with {error, reason}, remove it from the active_p list", %{
    server_id: server_id
  } do

    pid = :c.pid(0, 250, 0)
    pid2 = :c.pid(0, 250, 1)
    ref = make_ref()
    ref2 = make_ref()

    target_date = Date.utc_today()
    subscriptions = []
    active_p = %{pid => {ref, server_id}, pid2 => {ref2, server_id}}
    state = %Collector.GenCollector{target_date: target_date, subscriptions: subscriptions, active_p: active_p}

    {:noreply, new_state, {:continue, :is_finished}} = Collector.GenCollector.handle_cast({:ok, server_id, pid}, state)
    assert(new_state.active_p == %{pid2 => {ref2, server_id}})
  end

  test "If a server_id process crash during its collection, relaunch it and update the active_p with the new pid", %{
    server_id: server_id
  } do
    pid = :c.pid(0, 250, 0)
    ref = make_ref()

    target_date = Date.utc_today()
    subscriptions = [self()]
    active_p = %{pid => {ref, server_id}}
    down_msg = {:DOWN, ref, :process, pid, :error}
    state = %Collector.GenCollector{target_date: target_date, subscriptions: subscriptions, active_p: active_p}

    {:noreply, new_state, {:continue, :is_finished}} = Collector.GenCollector.handle_info(down_msg, state)
    [pid2] = Map.keys(new_state.active_p)
    {ref2, server_id2} = Map.get(new_state.active_p, pid2)
    assert(server_id == server_id2)
    assert(is_pid(pid2))
    assert(is_reference(ref2))
    assert(pid2 != pid)
    assert(ref2 != ref)
  end

  test "A collection ends where the last active_p process finish", %{
    server_id: server_id
  } do

    pid = :c.pid(0, 250, 0)
    pid2 = :c.pid(0, 250, 1)
    ref = make_ref()
    ref2 = make_ref()

    target_date = Date.utc_today()
    subscriptions = [self()]
    active_p = %{pid => {ref, server_id}, pid2 => {ref2, server_id}}
    state = %Collector.GenCollector{target_date: target_date, subscriptions: subscriptions, active_p: active_p}

    {:noreply, new_state, {:continue, :is_finished}} = Collector.GenCollector.handle_cast({:ok, server_id, pid}, state)
    Collector.GenCollector.handle_continue(:is_finished, new_state)
    refute_receive({:collector_event, :collection_finished}, 5_000)
    {:noreply, new_state2, {:continue, :is_finished}} = Collector.GenCollector.handle_cast({:ok, server_id, pid2}, new_state)
    assert(new_state2.active_p == %{})
    Collector.GenCollector.handle_continue(:is_finished, new_state2)
    assert_receive({:collector_event, :collection_finished}, 5_000)
  end

  # Supervisor.Worker tests
  @tag :tmp_dir
  test "Collector.Supervisor.Worker.start_child launch a GenWorker which runs a DAG", %{
    tmp_dir: root_folder,
    server_id: server_id
  } do
    Application.stop(:collector)
    Application.start(:collector)

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


  # GenWorker tests
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
