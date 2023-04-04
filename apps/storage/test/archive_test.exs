defmodule Storage.ArchiveTest do
  use ExUnit.Case

  setup_all do
    flow_options = {"raw_snapshot", ".c6bert"}
    %{server_id: "https://ts8.x1.europe.travian.com", flow_options: flow_options}
  end

  @tag :tmp_dir
  test "Servers with 7 days without a new raw_snapshot are candidates for being close", %{
    tmp_dir: root_folder,
    server_id: server_id,
    flow_options: flow_options
  } do
    content = "alskdjflaskdjf"
    target_date = Date.utc_today()
    target_date_7 = Date.add(target_date, -7)

    :ok =
      Storage.store(root_folder, server_id, flow_options, content, Date.add(target_date_7, -4))

    :ok =
      Storage.store(root_folder, server_id, flow_options, content, Date.add(target_date_7, -3))

    :ok =
      Storage.store(root_folder, server_id, flow_options, content, Date.add(target_date_7, -2))

    :ok =
      Storage.store(root_folder, server_id, flow_options, content, Date.add(target_date_7, -1))

    :ok = Storage.store(root_folder, server_id, flow_options, content, target_date_7)

    assert(
      Storage.Archive.candidate_for_closing?(root_folder, server_id, target_date) == {:ok, true}
    )

    :ok = Storage.store(root_folder, server_id, flow_options, content, target_date)

    assert(
      Storage.Archive.candidate_for_closing?(root_folder, server_id, target_date) == {:ok, false}
    )
  end

  @tag :tmp_dir
  test "Storage.Archive.candidate_for_closing? returns {:error, \"no server_id\"} if there is no server_id",
       %{
         tmp_dir: root_folder,
         server_id: server_id
       } do
    target_date = Date.utc_today()

    assert(
      Storage.Archive.candidate_for_closing?(root_folder, server_id, target_date) ==
        {:error, "no server_id"}
    )
  end

  @tag :tmp_dir
  test "move_to_archve move the server_id folder from server to archive and append to it a suffix",
       %{
         tmp_dir: root_folder,
         server_id: server_id,
         flow_options: flow_options
       } do
    content = "alskdjflaskdjf"
    target_date = Date.utc_today()
    target_date_7 = Date.add(target_date, -7)

    :ok =
      Storage.store(root_folder, server_id, flow_options, content, Date.add(target_date_7, -4))

    :ok =
      Storage.store(root_folder, server_id, flow_options, content, Date.add(target_date_7, -3))

    :ok =
      Storage.store(root_folder, server_id, flow_options, content, Date.add(target_date_7, -2))

    :ok =
      Storage.store(root_folder, server_id, flow_options, content, Date.add(target_date_7, -1))

    :ok =
      Storage.store(
        root_folder,
        server_id,
        {"blabla", ".json"},
        content,
        Date.add(target_date_7, -4)
      )

    :ok =
      Storage.store(
        root_folder,
        server_id,
        {"blabla", ".json"},
        content,
        Date.add(target_date_7, -3)
      )

    :ok =
      Storage.store(
        root_folder,
        server_id,
        {"blabla", ".json"},
        content,
        Date.add(target_date_7, -2)
      )

    :ok =
      Storage.store(
        root_folder,
        server_id,
        {"blabla", ".json"},
        content,
        Date.add(target_date_7, -1)
      )

    {:ok, server_id_suffix} = Storage.Archive.move_to_archive(root_folder, server_id)

    assert(
      Storage.exist?(
        root_folder,
        {:archive, server_id_suffix},
        flow_options,
        Date.add(target_date_7, -4)
      )
    )

    assert(
      Storage.exist?(
        root_folder,
        {:archive, server_id_suffix},
        flow_options,
        Date.add(target_date_7, -3)
      )
    )

    assert(
      Storage.exist?(
        root_folder,
        {:archive, server_id_suffix},
        flow_options,
        Date.add(target_date_7, -2)
      )
    )

    assert(
      Storage.exist?(
        root_folder,
        {:archive, server_id_suffix},
        flow_options,
        Date.add(target_date_7, -1)
      )
    )

    assert(
      Storage.exist?(
        root_folder,
        {:archive, server_id_suffix},
        {"blabla", ".json"},
        Date.add(target_date_7, -4)
      )
    )

    assert(
      Storage.exist?(
        root_folder,
        {:archive, server_id_suffix},
        {"blabla", ".json"},
        Date.add(target_date_7, -3)
      )
    )

    assert(
      Storage.exist?(
        root_folder,
        {:archive, server_id_suffix},
        {"blabla", ".json"},
        Date.add(target_date_7, -2)
      )
    )

    assert(
      Storage.exist?(
        root_folder,
        {:archive, server_id_suffix},
        {"blabla", ".json"},
        Date.add(target_date_7, -1)
      )
    )

    refute(Storage.exist?(root_folder, server_id, flow_options, Date.add(target_date_7, -4)))
    refute(Storage.exist?(root_folder, server_id, flow_options, Date.add(target_date_7, -3)))
    refute(Storage.exist?(root_folder, server_id, flow_options, Date.add(target_date_7, -2)))
    refute(Storage.exist?(root_folder, server_id, flow_options, Date.add(target_date_7, -1)))

    refute(
      Storage.exist?(root_folder, server_id, {"blabla", ".json"}, Date.add(target_date_7, -4))
    )

    refute(
      Storage.exist?(root_folder, server_id, {"blabla", ".json"}, Date.add(target_date_7, -3))
    )

    refute(
      Storage.exist?(root_folder, server_id, {"blabla", ".json"}, Date.add(target_date_7, -2))
    )

    refute(
      Storage.exist?(root_folder, server_id, {"blabla", ".json"}, Date.add(target_date_7, -1))
    )
  end

  @tag :tmp_dir
  test "if there is the same server_id in the archive, increase the suffix", %{
    tmp_dir: root_folder,
    server_id: server_id,
    flow_options: flow_options
  } do
    content = "alskdjflaskdjf"
    target_date = Date.utc_today()
    target_date_7 = Date.add(target_date, -7)

    :ok =
      Storage.store(root_folder, server_id, flow_options, content, Date.add(target_date_7, -4))

    {:ok, server_id_suffix_1} = Storage.Archive.move_to_archive(root_folder, server_id)
    assert(server_id_suffix_1 == "#{server_id}__0")

    :ok =
      Storage.store(root_folder, server_id, flow_options, content, Date.add(target_date_7, -4))

    {:ok, server_id_suffix_2} = Storage.Archive.move_to_archive(root_folder, server_id)
    assert(server_id_suffix_2 == "#{server_id}__1")

    assert(
      Storage.exist?(
        root_folder,
        {:archive, server_id_suffix_1},
        flow_options,
        Date.add(target_date_7, -4)
      )
    )

    assert(
      Storage.exist?(
        root_folder,
        {:archive, server_id_suffix_2},
        flow_options,
        Date.add(target_date_7, -4)
      )
    )
  end
end
