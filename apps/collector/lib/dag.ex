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
    Logger.debug(%{
      msg: "Starting full Collector.DAG for #{server_id} at #{target_date}",
      server_id: server_id,
      target_date: target_date,
      target_dt: DateTime.new!(target_date, Time.new!(0, 0, 0)),
      current_dt: DateTime.utc_now()
    })

    f = fn ->
      Collector.Feed.run_feed(root_folder, server_id, target_date, Collector.RawSnapshot)
    end

    case Retryable.retryable([on: :error, tries: attemps], f) do
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
        case run_without_fetch(root_folder, server_id, target_date) do
          {:error, reason} ->
            Logger.error(%{
              msg: "Unable to run internal DAG for #{server_id} at #{target_date}",
              server_id: server_id,
              target_date: target_date,
              target_dt: DateTime.new!(target_date, Time.new!(0, 0, 0)),
              current_dt: DateTime.utc_now(),
              reason: reason
            })

            {:error, reason}

          :ok ->
            push_to_mnesia(root_folder, server_id, target_date)
        end
    end
  end

  @spec run_without_fetch(
          root_folder :: String.t(),
          server_id :: TTypes.server_id() | {:archive, TTypes.server_id()},
          target_date :: Date.t()
        ) :: :ok | {:error, any()}
  def run_without_fetch(root_folder, {:archive, server_id}, target_date),
    do: run_without_fetch(root_folder, server_id, target_date)

  def run_without_fetch(root_folder, server_id, target_date) do
    Logger.debug(%{
      msg: "Starting internal Collector.DAG for #{server_id} at #{target_date}",
      server_id: server_id,
      target_date: target_date,
      target_dt: DateTime.new!(target_date, Time.new!(0, 0, 0)),
      current_dt: DateTime.utc_now()
    })

    with(
      {:a, :ok} <-
        {:a, Collector.Feed.run_feed(root_folder, server_id, target_date, Collector.Snapshot)},
      Logger.debug(%{
        msg: "Snapshot finished for #{server_id} at #{target_date}",
        server_id: server_id,
        target_date: target_date,
        target_dt: DateTime.new!(target_date, Time.new!(0, 0, 0)),
        current_dt: DateTime.utc_now()
      }),
      {:b, :ok} <-
        {:b, Collector.Feed.run_feed(root_folder, server_id, target_date, Collector.AggPlayers)},
      Logger.debug(%{
        msg: "AggPlayers finished for #{server_id} at #{target_date}",
        server_id: server_id,
        target_date: target_date,
        target_dt: DateTime.new!(target_date, Time.new!(0, 0, 0)),
        current_dt: DateTime.utc_now()
      }),
      {:c, :ok} <-
        {:c, Collector.Feed.run_feed(root_folder, server_id, target_date, Collector.AggServer)},
      Logger.debug(%{
        msg: "AggServer finished for #{server_id} at #{target_date}",
        server_id: server_id,
        target_date: target_date,
        target_dt: DateTime.new!(target_date, Time.new!(0, 0, 0)),
        current_dt: DateTime.utc_now()
      }),
      {:d, :ok} <-
        {:d,
         Collector.Feed.run_feed(root_folder, server_id, target_date, Collector.MedusaPredInput)},
      Logger.debug(%{
        msg: "MedusaPredInput finished for #{server_id} at #{target_date}",
        server_id: server_id,
        target_date: target_date,
        target_dt: DateTime.new!(target_date, Time.new!(0, 0, 0)),
        current_dt: DateTime.utc_now()
      }),
      {:e, :ok} <-
        {:e, Collector.Feed.run_feed(root_folder, server_id, target_date, Collector.MedusaTrain)},
      Logger.debug(%{
        msg: "MedusaTrain finished for #{server_id} at #{target_date}",
        server_id: server_id,
        target_date: target_date,
        target_dt: DateTime.new!(target_date, Time.new!(0, 0, 0)),
        current_dt: DateTime.utc_now()
      }),
      {:f, :ok} <-
        {:f,
         Collector.Feed.run_feed(
           root_folder,
           server_id,
           target_date,
           Collector.MedusaPredOutput,
           %{"medusa_gen_port" => Collector.MedusaPredOutput.GenPort}
         )},
      Logger.debug(%{
        msg: "MedusaPredOutput finished for #{server_id} at #{target_date}",
        server_id: server_id,
        target_date: target_date,
        target_dt: DateTime.new!(target_date, Time.new!(0, 0, 0)),
        current_dt: DateTime.utc_now()
      }),
      {:g, :ok} <-
        {:g,
         Collector.Feed.run_feed(
           root_folder,
           server_id,
           target_date,
           Collector.SMedusaPred
         )},
      Logger.debug(%{
        msg: "SMedusaPred finished for #{server_id} at #{target_date}",
        server_id: server_id,
        target_date: target_date,
        target_dt: DateTime.new!(target_date, Time.new!(0, 0, 0)),
        current_dt: DateTime.utc_now()
      }),
      {:h, :ok} <-
        {:h,
         Collector.Feed.run_feed(
           root_folder,
           server_id,
           target_date,
           Collector.MedusaScore
         )},
      Logger.debug(%{
        msg: "MedusaScore finished for #{server_id} at #{target_date}",
        server_id: server_id,
        target_date: target_date,
        target_dt: DateTime.new!(target_date, Time.new!(0, 0, 0)),
        current_dt: DateTime.utc_now()
      }),
      {:i, :ok} <-
        {:i,
         Collector.Feed.run_feed(
           root_folder,
           server_id,
           target_date,
           Collector.ApiMapSql
         )},
      Logger.debug(%{
        msg: "ApiMapSql finished for #{server_id} at #{target_date}",
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

  @spec push_to_mnesia(
          root_folder :: String.t(),
          server_id :: TTypes.server_id() | {:archive, TTypes.server_id()},
          target_date :: Date.t()
        ) :: :ok | {:error, any()}
  def push_to_mnesia(root_folder, server_id, target_date) do
    Logger.debug(%{
      msg: "Starting pushing to Mnesia for #{server_id} at #{target_date}",
      server_id: server_id,
      target_date: target_date,
      target_dt: DateTime.new!(target_date, Time.new!(0, 0, 0)),
      current_dt: DateTime.utc_now()
    })

    with(
      {:a, :ok} <-
        {:a,
         Collector.Feed.insert_in_table(
           root_folder,
           server_id,
           target_date,
           Collector.SMedusaPred
         )},
      Logger.debug(%{
        msg: "SMedusaPred inserted in Mnesia #{server_id} at #{target_date}",
        server_id: server_id,
        target_date: target_date,
        target_dt: DateTime.new!(target_date, Time.new!(0, 0, 0)),
        current_dt: DateTime.utc_now()
      }),
      {:b, :ok} <-
        {:b,
         Collector.Feed.insert_in_table(
           root_folder,
           server_id,
           target_date,
           Collector.ApiMapSql
         )},
      Logger.debug(%{
        msg: "ApiMapSql inserted in Mnesia #{server_id} at #{target_date}",
        server_id: server_id,
        target_date: target_date,
        target_dt: DateTime.new!(target_date, Time.new!(0, 0, 0)),
        current_dt: DateTime.utc_now()
      }),
      {:c, :ok} <-
        {:c,
         Collector.Feed.insert_in_table(
           root_folder,
           server_id,
           target_date,
           Collector.SServer
         )},
      Logger.debug(%{
        msg: "SServer inserted in Mnesia #{server_id} at #{target_date}",
        server_id: server_id,
        target_date: target_date,
        target_dt: DateTime.new!(target_date, Time.new!(0, 0, 0)),
        current_dt: DateTime.utc_now()
      })
    ) do
      Logger.info(%{
        msg: "Push to Mnesia done for #{server_id} at #{target_date}",
        server_id: server_id,
        target_date: target_date,
        target_dt: DateTime.new!(target_date, Time.new!(0, 0, 0)),
        current_dt: DateTime.utc_now()
      })
    else
      {step, error} ->
        Logger.error(%{
          msg: "Unable to push to Mnesia for #{server_id} at #{target_date}",
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

  @spec launch_collection(
          root_folder :: String.t(),
          servers_list :: [TTypes.server_id()],
          target_date :: Date.t(),
          launch_options :: map()
        ) :: [:ok | {:error, any()}]
  def launch_collection(root_folder, servers_list, target_date, launch_options) do
    f = fn server_id ->
      run(
        root_folder,
        server_id,
        target_date,
        launch_options[:attemps],
        launch_options[:min],
        launch_options[:max]
      )
    end

    Flow.from_enumerable(servers_list, max_demand: 1, stages: launch_options[:stages])
    |> Flow.map(f)
    |> Enum.to_list()
  end

  @spec reload(root_folder :: String.t(), server_id :: TTypes.server_id()) :: :ok
  def reload(root_folder, server_id) do
    available_dates =
      Storage.list_dates(root_folder, server_id, Collector.RawSnapshot.options())
      |> Enum.sort({:asc, Date})

    for date <- available_dates, do: run_without_fetch(root_folder, server_id, date)
    :ok
  end

  def feed_reload(root_folder, server_id, feed) do
    available_dates =
      Storage.list_dates(root_folder, server_id, Collector.RawSnapshot.options())
      |> Enum.sort({:asc, Date})

    for date <- available_dates, do: Collector.Feed.run_feed(root_folder, server_id, date, feed)
    :ok
  end

  @spec full_flow_reload!(root_folder :: String.t(), max_demand :: pos_integer()) :: :ok
  def full_flow_reload!(root_folder, max_demand \\ 1) do
    # Reload first active servers
    Storage.list_servers(root_folder)
    |> Flow.from_enumerable(max_demand: max_demand)
    |> Flow.map(fn server_id -> reload(root_folder, server_id) end)
    |> Enum.to_list()

    # just for triggering Flow

    # Then reload archive
    Storage.list_servers(root_folder, :archive)
    |> Enum.map(fn server_id -> {:archive, server_id} end)
    |> Flow.from_enumerable(max_demand: max_demand)
    |> Flow.map(fn server_id -> reload(root_folder, server_id) end)
    |> Enum.to_list()

    :ok
  end

  def full_feed_reload!(root_folder, feed, max_demand \\ 1) do
    # Reload first active servers
    Storage.list_servers(root_folder)
    |> Flow.from_enumerable(max_demand: max_demand)
    |> Flow.map(fn server_id -> feed_reload(root_folder, server_id, feed) end)
    |> Enum.to_list()

    # just for triggering Flow

    # Then reload archive
    Storage.list_servers(root_folder, :archive)
    |> Enum.map(fn server_id -> {:archive, server_id} end)
    |> Flow.from_enumerable(max_demand: max_demand)
    |> Flow.map(fn server_id -> feed_reload(root_folder, server_id, feed) end)
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
    case f.() do
      :ok ->
        :ok

      {:error, reason} ->
        # sleep = compute_sleep(min, max)
        # :timer.sleep(sleep)
        retry(f, min, max, attemps, tries + 1, reason)
    end
  end

  defp compute_sleep(min, max) when max >= min do
    :rand.uniform(max - min) + min
  end
end
