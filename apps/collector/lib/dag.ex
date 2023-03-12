defmodule Collector.DAG do
  require Logger

  @moduledoc """
  Direct Aciclyc Graph for the daily collection. It runs all the flows in the specific order.
  """

  @spec run(
          root_folder :: String.t(),
          server_id :: TTypes.server_id(),
          target_date :: Date.t(),
          attemps :: pos_integer(),
          min :: pos_integer(),
          max :: pos_integer
        ) :: :ok | {:error, any()}
  def run(root_folder, server_id, target_date, attemps, min, max) do
    Logger.info(%{
      msg: "Starting full Collector.DAG for #{server_id} at #{target_date}",
      server_id: server_id,
      target_date: target_date,
      target_dt: DateTime.new!(target_date, Time.new!(0, 0, 0)),
      current_dt: DateTime.utc_now()
    })

    f = fn -> Collector.RawSnapshot.run(root_folder, server_id, target_date) end

    case retry(f, min, max, attemps) do
      {:error, reason} ->
        Logger.error(%{
          msg: "Unable to fetch map_sql for #{server_id} at #{target_date}",
          server_id: server_id,
          target_date: target_date,
          target_dt: DateTime.new!(target_date, Time.new!(0, 0, 0)),
          current_dt: DateTime.utc_now(),
          reason: reason
        })

        {:error, reason}

      :ok ->
        run_without_fetch(root_folder, server_id, target_date)
    end
  end

  @spec run_without_fetch(
          root_folder :: String.t(),
          server_id :: TTypes.server_id(),
          target_date :: Date.t()
        ) :: :ok | {:error, any()}
  def run_without_fetch(root_folder, server_id, target_date) do
    Logger.info(%{
      msg: "Starting internal Collector.DAG for #{server_id} at #{target_date}",
      server_id: server_id,
      target_date: target_date,
      target_dt: DateTime.new!(target_date, Time.new!(0, 0, 0)),
      current_dt: DateTime.utc_now()
    })

    with(
      {:a, :ok} <- {:a, Collector.Snapshot.run(root_folder, server_id, target_date)},
      Logger.debug(%{
        msg: "Snapshot finished for #{server_id} at #{target_date}",
        server_id: server_id,
        target_date: target_date,
        target_dt: DateTime.new!(target_date, Time.new!(0, 0, 0)),
        current_dt: DateTime.utc_now()
      }),
      {:b, :ok} <- {:b, Collector.AggPlayers.run(root_folder, server_id, target_date)},
      Logger.debug(%{
        msg: "AggPlayers finished for #{server_id} at #{target_date}",
        server_id: server_id,
        target_date: target_date,
        target_dt: DateTime.new!(target_date, Time.new!(0, 0, 0)),
        current_dt: DateTime.utc_now()
      })
    ) do
      Logger.info(%{
        msg: "Collector.DAG finished for #{server_id} at #{target_date}",
        server_id: server_id,
        target_date: target_date,
        target_dt: DateTime.new!(target_date, Time.new!(0, 0, 0)),
        current_dt: DateTime.utc_now()
      })

      :ok
    else
      {step, error} ->
        Logger.error(%{
          msg: "Unable to run internal Collector.DAG for #{server_id} at #{target_date}",
          server_id: server_id,
          target_date: target_date,
          target_dt: DateTime.new!(target_date, Time.new!(0, 0, 0)),
          current_dt: DateTime.utc_now(),
          step: step,
          reason: error
        })

        error
    end
  end

  @spec reload(root_folder :: String.t(), server_id :: TTypes.server_id()) :: :ok
  def reload(root_folder, server_id) do
    available_dates =
      Storage.list_dates(root_folder, server_id, Collector.RawSnapshot.options())
      |> Enum.sort({:asc, Date})

    for date <- available_dates, do: run_without_fetch(root_folder, server_id, date)
    :ok
  end

  @spec full_flow_reload!(root_folder :: String.t()) :: :ok
  def full_flow_reload!(root_folder) do
    Storage.list_servers(root_folder)
    |> Flow.from_enumerable(max_demand: 1)
    |> Flow.map(fn server_id -> reload(root_folder, server_id) end)
    |> Enum.to_list()

    :ok
  end

  defp retry(f, min, max, attemps) when min <= max do
    retry(f, min, max, attemps, 0, nil)
  end

  defp retry(_f, _min, _max, attemps, attemps, error) do
    {:error, {:max_retries, error}}
  end

  defp retry(f, min, max, attemps, tries, _error) when tries < attemps do
    sleep = compute_sleep(min, max)
    :timer.sleep(sleep)

    case f.() do
      :ok -> :ok
      {:error, reason} -> retry(f, min, max, attemps, tries + 1, reason)
    end
  end

  defp compute_sleep(min, max) when max >= min do
    :rand.uniform(max - min) + min
  end
end
