defmodule Collector.Scripts do
  def set_up_mnesia(nodes, master_node) do
    :rpc.multicall(nodes, :application, :stop, [:mnesia])

    with(
      :ok <- :mnesia.delete_schema(nodes),
      :ok <- :mnesia.create_schema(nodes),
      :rpc.multicall(nodes, :application, :start, [:mnesia]),
      creation_results =
        Enum.map(Collector.Feed.list_table_modules(), &Collector.Feed.create_table(nodes, &1)),
      true <- Enum.all?(creation_results, fn {mnesia_result, _} -> mnesia_result == :atomic end),
      :ok <- :mnesia.set_master_nodes([master_node])
    ) do
      :ok
    else
      false -> {:error, "unable to create tables"}
      reason -> {:error, reason}
    end
  end

  def clear_tables() do
    Collector.Feed.list_table_modules()
    |> Enum.map(fn x -> String.to_existing_atom(elem(x.options(), 0)) end)
    |> Enum.each(fn table_name -> :mnesia.clear_table(table_name) end)
  end

  def push_to_mnesia(root_folder, target_date) do
    stages = Application.fetch_env!(:collector, :stages)

    clear_tables()

    table_has_day = fn server_id, table_module ->
      target_date in Storage.list_dates(root_folder, server_id, table_module.options())
    end

    available_modules =
      Collector.Feed.list_table_modules() |> Enum.filter(&(&1 != Collector.SServer))

    server_has_all_tables = fn server_id ->
      Enum.map(available_modules, &table_has_day.(server_id, &1))
      |> Enum.all?()
    end

    f = fn server_id -> Collector.DAG.push_to_mnesia(root_folder, server_id, target_date) end

    Storage.list_servers(root_folder)
    |> Enum.filter(&server_has_all_tables.(&1))
    |> Flow.from_enumerable(max_demand: 1, stages: stages)
    |> Flow.map(f)
    |> Enum.to_list()

    :ok
  end

  def copy_all_snapshot_raw_to_a_folder(root_folder, dst_folder) do
    File.mkdir_p!(dst_folder)

    servers = Storage.list_servers(root_folder) ++ Storage.list_servers(root_folder, :archive)

    servers
    |> Enum.flat_map(fn s -> zip_date(root_folder, s, dst_folder, Collector.RawSnapshot) end)
    |> Enum.each(&open_and_copy_raw/1)
  end

  defp open_and_copy_raw({dst_folder, root_folder, server_id, target_date}) do
    {:ok, raw_snapshot} =
      Collector.Feed.open(root_folder, server_id, target_date, Collector.RawSnapshot)

    :ok =
      Collector.Feed.store(
        dst_folder,
        server_id,
        target_date,
        raw_snapshot,
        Collector.RawSnapshot
      )
  end

  def copy_all_medusa_train_to_a_folder(root_folder, dst_folder) do
    File.mkdir_p!(dst_folder)

    # servers = Storage.list_servers(root_folder) ++ Storage.list_servers(root_folder, :archive)
    servers = Storage.list_servers(root_folder)

    servers
    |> Enum.flat_map(fn s -> zip_date(root_folder, s, dst_folder, Collector.MedusaTrain) end)
    |> Enum.each(&read_json_write/1)
  end

  defp zip_date(root_folder, server_id, dst_folder, module) do
    dates = Storage.list_dates(root_folder, server_id, module.options())
    for date <- dates, do: {dst_folder, root_folder, server_id, date}
  end

  defp read_json_write({dst_folder, root_folder, server_id, target_date}) do
    {:ok, medusa_train} =
      Collector.Feed.open(root_folder, server_id, target_date, Collector.MedusaTrain)

    json = Jason.encode!(medusa_train)

    File.write!(
      "#{dst_folder}/#{TTypes.server_id_to_path(server_id)}_#{Date.to_iso8601(target_date, :basic)}.json",
      json
    )
  end

  # def reload_all_servers(root_folder) do
  #   reload_active_servers(root_folder)
  #   reload_archived_servers(root_folder)
  # end

  # def reload_active_servers(root_folder), do: :ok
  # def reload_archived_servers(root_folder), do: :ok
end
