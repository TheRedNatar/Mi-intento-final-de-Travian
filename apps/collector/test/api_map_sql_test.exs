defmodule Collector.ApiMapSqlTest do
  use ExUnit.Case

  setup_all do
    :ok = Application.ensure_started(:collector)
    :ok = Collector.Scripts.set_up_mnesia([Node.self()], Node.self())
    on_exit(fn -> Application.stop(:collector) end)
    %{server_id: "https://ts6.x1.america.travian.com"}
  end

  @tag :tmp_dir
  test "Only remove the mnesia api_map_sql rows if they are older than the retention parameter",
       %{tmp_dir: root_folder} do
    target_date = Date.utc_today()
    server_id = "server1"

    add_date = fn x -> Date.add(target_date, x) end
    date_to_greg = fn x -> Date.to_gregorian_days(add_date.(x)) end

    fake_content = fn x ->
      %Collector.ApiMapSql{server_id: server_id, target_date: add_date.(x), rows: []}
    end

    assert(
      :ok ==
        Collector.Feed.store(
          root_folder,
          server_id,
          add_date.(-3),
          fake_content.(-3),
          Collector.ApiMapSql
        )
    )

    assert(
      :ok ==
        Collector.Feed.store(
          root_folder,
          server_id,
          add_date.(-2),
          fake_content.(-2),
          Collector.ApiMapSql
        )
    )

    assert(
      :ok ==
        Collector.Feed.store(
          root_folder,
          server_id,
          add_date.(-1),
          fake_content.(-1),
          Collector.ApiMapSql
        )
    )

    assert(
      :ok ==
        Collector.Feed.store(
          root_folder,
          server_id,
          add_date.(0),
          fake_content.(0),
          Collector.ApiMapSql
        )
    )

    assert(
      :ok ==
        Collector.Feed.store(
          root_folder,
          server_id,
          add_date.(1),
          fake_content.(1),
          Collector.ApiMapSql
        )
    )

    assert(
      :ok ==
        Collector.Feed.insert_in_table(root_folder, server_id, add_date.(-3), Collector.ApiMapSql)
    )

    assert(
      :ok ==
        Collector.Feed.insert_in_table(root_folder, server_id, add_date.(-2), Collector.ApiMapSql)
    )

    assert(
      :ok ==
        Collector.Feed.insert_in_table(root_folder, server_id, add_date.(-1), Collector.ApiMapSql)
    )

    assert(
      :ok ==
        Collector.Feed.insert_in_table(root_folder, server_id, add_date.(0), Collector.ApiMapSql)
    )

    assert(
      :ok ==
        Collector.Feed.insert_in_table(root_folder, server_id, add_date.(1), Collector.ApiMapSql)
    )

    expected_dates = [
      date_to_greg.(-3),
      date_to_greg.(-2),
      date_to_greg.(-1),
      date_to_greg.(0),
      date_to_greg.(1)
    ]

    assert(
      expected_dates ==
        Enum.sort(:mnesia.activity(:sync_transaction, fn -> :mnesia.all_keys(:api_map_sql) end))
    )

    assert(:ok == Collector.ApiMapSql.clean(target_date, %{"retention_period" => 4}))

    assert(
      expected_dates ==
        Enum.sort(:mnesia.activity(:sync_transaction, fn -> :mnesia.all_keys(:api_map_sql) end))
    )

    assert(:ok == Collector.ApiMapSql.clean(target_date, %{"retention_period" => 3}))

    assert(
      expected_dates ==
        Enum.sort(:mnesia.activity(:sync_transaction, fn -> :mnesia.all_keys(:api_map_sql) end))
    )

    assert(:ok == Collector.ApiMapSql.clean(target_date, %{"retention_period" => 2}))

    assert(
      Enum.drop(expected_dates, 1) ==
        Enum.sort(:mnesia.activity(:sync_transaction, fn -> :mnesia.all_keys(:api_map_sql) end))
    )

    assert(:ok == Collector.ApiMapSql.clean(target_date, %{"retention_period" => 1}))

    assert(
      Enum.drop(expected_dates, 2) ==
        Enum.sort(:mnesia.activity(:sync_transaction, fn -> :mnesia.all_keys(:api_map_sql) end))
    )
  end
end
