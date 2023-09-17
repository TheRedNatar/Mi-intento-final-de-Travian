defmodule Collector.SServer do
  @behaviour Collector.Feed

  @table_name :s_server

  @impl true
  def table_config() do
    options = [
      attributes: [
        :server_id,
        :target_date,
        :struct
      ],
      type: :set
    ]

    {@table_name, options}
  end

  @impl true
  def clean(_target_date, _options) do
    case :mnesia.clear_table(@table_name) do
      {:atomic, :ok} -> :ok
      error = {:aborted, _reason} -> {:error, error}
    end
  end

  @impl true
  def insert(root_folder, server_id, target_date, _) do
    with(
      {:a, {:ok, agg_server}} <-
        {:a, Collector.Feed.open(root_folder, server_id, target_date, Collector.AggServer)},
      func = fn ->
        insert_record(agg_server)
        :ok
      end,
      {:b, :ok} <- {:b, :mnesia.activity(:sync_transaction, func)}
    ) do
      :ok
    else
      {:a, {:error, reason}} ->
        {:error, {"Unable to open target_date s_server", reason}}

      {:b, {:aborted, reason}} ->
        {:error, {"Unable to insert on Mnesia", reason}}
    end
  end

  defp insert_record(x),
    do: :mnesia.write({@table_name, x.server_id, DateTime.to_date(x.target_dt), x})

  @impl true
  def options(), do: {"s_server", ".c6bert"}
end
