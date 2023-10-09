defmodule CollectorArchTest do
  use ExUnit.Case

  @moduletag :capture_log

  setup_all do
    :ok = Application.ensure_started(:collector)
    :ok = Collector.Scripts.set_up_mnesia([Node.self()], Node.self())
    on_exit(fn -> wait_on_stop() end)
    %{server_id: "https://ts6.x1.america.travian.com"}
  end

  defp wait_on_stop() do
    :ok = Application.stop(:collector)
    Process.sleep(1_000)
  end

  @tag :tmp_dir
  test "Collector.DAG.run creates the tables and push them to Mnesia", %{
    tmp_dir: root_folder,
    server_id: server_id
  } do
    target_date = Date.utc_today()
    tomorrow = Date.add(target_date, 1)
    attemps = 1
    min = 1_000
    max = 2_000

    assert(:ok == Collector.DAG.run(root_folder, server_id, target_date, attemps, min, max))

    assert(Storage.exist?(root_folder, server_id, Collector.RawSnapshot.options(), target_date))
    assert(Storage.exist?(root_folder, server_id, Collector.Snapshot.options(), target_date))
    assert(Storage.exist?(root_folder, server_id, Collector.AggPlayers.options(), target_date))
    assert(Storage.exist?(root_folder, server_id, Collector.AggServer.options(), target_date))

    assert(
      Storage.exist?(root_folder, server_id, Collector.MedusaPredInput.options(), target_date)
    )

    assert(
      Storage.exist?(root_folder, server_id, Collector.MedusaPredOutput.options(), target_date)
    )

    assert(Storage.exist?(root_folder, server_id, Collector.SMedusaPred.options(), target_date))

    ##### Next Day

    assert(:ok == Collector.DAG.run(root_folder, server_id, tomorrow, attemps, min, max))

    assert(Storage.exist?(root_folder, server_id, Collector.RawSnapshot.options(), tomorrow))
    assert(Storage.exist?(root_folder, server_id, Collector.Snapshot.options(), tomorrow))
    assert(Storage.exist?(root_folder, server_id, Collector.AggPlayers.options(), tomorrow))
    assert(Storage.exist?(root_folder, server_id, Collector.AggServer.options(), tomorrow))
    assert(Storage.exist?(root_folder, server_id, Collector.MedusaPredInput.options(), tomorrow))
    assert(Storage.exist?(root_folder, server_id, Collector.MedusaTrain.options(), tomorrow))
    assert(Storage.exist?(root_folder, server_id, Collector.MedusaPredOutput.options(), tomorrow))
    assert(Storage.exist?(root_folder, server_id, Collector.SMedusaPred.options(), tomorrow))
    assert(Storage.exist?(root_folder, server_id, Collector.MedusaScore.options(), tomorrow))
  end

  @tag :tmp_dir
  test "Collector.DAG.run runs and do nothing with a bad_server_id but it doesn't raise", %{
    tmp_dir: root_folder
  } do
    target_date = Date.utc_today()
    server_id = "https://no_exists.travian.com"
    attemps = 1
    min = 1_000
    max = 2_000

    {:error, _reason} = Collector.DAG.run(root_folder, server_id, target_date, attemps, min, max)

    assert(!Storage.exist?(root_folder, server_id, Collector.RawSnapshot.options(), target_date))
    assert(!Storage.exist?(root_folder, server_id, Collector.Snapshot.options(), target_date))
    assert(!Storage.exist?(root_folder, server_id, Collector.AggPlayers.options(), target_date))
    assert(!Storage.exist?(root_folder, server_id, Collector.AggServer.options(), target_date))

    assert(
      !Storage.exist?(root_folder, server_id, Collector.MedusaPredInput.options(), target_date)
    )

    assert(
      !Storage.exist?(root_folder, server_id, Collector.MedusaPredOutput.options(), target_date)
    )

    assert(!Storage.exist?(root_folder, server_id, Collector.SMedusaPred.options(), target_date))
  end

  # GenArchive

  # @tag :tmp_dir
  # test "GenArchive is triggered once the collection is finished", %{
  #   tmp_dir: root_folder
  # } do
  #   server_id = "https://ts8.x10.europe.travian.com"
  #   content = "alskdjfalksdj"
  #   target_date = Date.utc_today()

  #   Collector.Feed.store(
  #     root_folder,
  #     server_id,
  #     Date.add(target_date, -8),
  #     content,
  #     Collector.RawSnapshot
  #   )

  #   Collector.Feed.store(
  #     root_folder,
  #     server_id,
  #     Date.add(target_date, -7),
  #     content,
  #     Collector.RawSnapshot
  #   )

  #   assert(Storage.list_servers(root_folder) == [server_id])

  #   state = %Collector.GenCollector{}

  #   Application.put_env(:collector, :root_folder, root_folder)
  #   Collector.GenCollector.handle_continue(:is_finished, state)

  #   # maybe capture log
  #   # sleep 5 secs but evaluate the condition in smaller steps
  #   Process.sleep(2_000)
  #   assert(Storage.list_servers(root_folder) == [])
  #   assert(Storage.list_servers(root_folder, :archive) == ["#{server_id}__0"])
  # end

  @tag :tmp_dir
  test "GenArchive.start_archiving evaluates the active servers and if they are condidates for archiving, it moves them to the archive folder",
       %{
         tmp_dir: root_folder
       } do
    server_id_1 = "https://ts8.x1.europe.travian.com"
    server_id_2 = "https://ts8.x10.europe.travian.com"
    content = "alskdjfalksdj"
    target_date = Date.utc_today()

    Collector.Feed.store(
      root_folder,
      server_id_1,
      Date.add(target_date, -1),
      content,
      Collector.RawSnapshot
    )

    Collector.Feed.store(root_folder, server_id_1, target_date, content, Collector.RawSnapshot)

    Collector.Feed.store(
      root_folder,
      server_id_2,
      Date.add(target_date, -8),
      content,
      Collector.RawSnapshot
    )

    Collector.Feed.store(
      root_folder,
      server_id_2,
      Date.add(target_date, -7),
      content,
      Collector.RawSnapshot
    )

    assert(:ok == Collector.GenArchive.start_archiving(root_folder, target_date))

    # maybe capture log
    # sleep 5 secs but evaluate the condition in smaller steps
    Process.sleep(1_000)

    assert(Storage.list_servers(root_folder) == [server_id_1])
    assert(Storage.list_servers(root_folder, :archive) == ["#{server_id_2}__0"])
  end
end
