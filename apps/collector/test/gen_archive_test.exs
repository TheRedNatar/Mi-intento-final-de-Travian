defmodule Collector.GenArchiveTest do
  use ExUnit.Case

  @tag :tmp_dir
  test "GenArchive.start_archiving evaluates the active servers and if they are condidates for archiving, it moves them to the archive folder",
       %{
         tmp_dir: root_folder
       } do
    server_id_1 = "https://ts8.x1.europe.travian.com"
    server_id_2 = "https://ts8.x10.europe.travian.com"
    content = "alskdjfalksdj"
    target_date = Date.utc_today()

    Collector.RawSnapshot.store(root_folder, server_id_1, content, Date.add(target_date, -1))
    Collector.RawSnapshot.store(root_folder, server_id_1, content, target_date)

    Collector.RawSnapshot.store(root_folder, server_id_2, content, Date.add(target_date, -8))
    Collector.RawSnapshot.store(root_folder, server_id_2, content, Date.add(target_date, -7))

    assert(:ok == Collector.GenArchive.start_archiving(root_folder, target_date))

    # maybe capture log
    # sleep 5 secs but evaluate the condition in smaller steps
    Process.sleep(1_000)

    assert(Storage.list_servers(root_folder) == [server_id_1])
    assert(Storage.list_servers(root_folder, :archive) == ["#{server_id_2}__0"])
  end
end
