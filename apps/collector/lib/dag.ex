defmodule Collector.DAG do
  require Logger

  @moduledoc """
  Direct Aciclyc Graph for the daily collection. It runs all the flows in the specific order.
  """

  @spec run(
          root_folder :: String.t(),
          server_id :: TTypes.server_id(),
          target_date :: Date.t(),
          server_metadata :: :travianmap_map.village_record(),
          attemps :: pos_integer()
        ) :: :ok | {:error, any()}
  def run(root_folder, server_id, target_date, server_metadata, attemps) do
    Logger.debug(%{
      msg: "Starting full Collector.DAG for #{server_id} at #{target_date}",
      server_id: server_id,
      target_date: target_date,
      target_dt: DateTime.new!(target_date, Time.new!(0, 0, 0)),
      server_metadata: server_metadata,
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
        case run_without_fetch(root_folder, server_id, target_date, server_metadata) do
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
          server_metadata :: :travianmap_map.village_record(),
          target_date :: Date.t()
        ) :: :ok | {:error, any()}
  def run_without_fetch(root_folder, {:archive, server_id}, target_date, server_metadata),
    do: run_without_fetch(root_folder, server_id, target_date, server_metadata)

  def run_without_fetch(root_folder, server_id, target_date, server_metadata) do
    Logger.debug(%{
      msg: "Starting internal Collector.DAG for #{server_id} at #{target_date}",
      server_id: server_id,
      target_date: target_date,
      target_dt: DateTime.new!(target_date, Time.new!(0, 0, 0)),
      current_dt: DateTime.utc_now()
    })

    options = %{
      "server_metadata" => server_metadata,
      "medusa_gen_port" => Collector.MedusaPredOutput.GenPort
    }

    pa_run = fn feed_module ->
      Collector.Utils.run_with_debug(root_folder, server_id, target_date, feed_module, options)
    end

    with(
      {1, :ok} <- {1, pa_run.(Collector.Snapshot)},
      {2, :ok} <- {2, pa_run.(Collector.ServerMetadata)},
      {3, :ok} <- {3, pa_run.(Collector.AggPlayers)},
      {4, :ok} <- {4, pa_run.(Collector.AggServer)},
      {5, :ok} <- {5, pa_run.(Collector.MedusaPredInput)},
      {6, :ok} <- {6, pa_run.(Collector.MedusaTrain)},
      {7, :ok} <- {7, pa_run.(Collector.MedusaPredOutput)},
      {8, :ok} <- {8, pa_run.(Collector.SMedusaPred)},
      {9, :ok} <- {9, pa_run.(Collector.MedusaScore)},
      {10, :ok} <- {10, pa_run.(Collector.ApiMapSql)}
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

    pa_insert = fn feed_module ->
      Collector.Utils.insert_with_debug(root_folder, server_id, target_date, feed_module, %{})
    end

    with(
      {1, :ok} <- {1, pa_insert.(Collector.SMedusaPred)},
      {2, :ok} <- {2, pa_insert.(Collector.ApiMapSql)},
      {3, :ok} <- {3, pa_insert.(Collector.SServer)}
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
          server_list :: [{:ok, :travianmap_map.village_record()} | {:error, [binary()]}],
          target_date :: Date.t(),
          launch_options :: map()
        ) :: [:ok | {:error, any()}]
  def launch_collection(root_folder, server_list, target_date, launch_options) do
    f = fn server_metadata ->
      server_id = Map.fetch!(server_metadata, :url)

      run(
        root_folder,
        server_id,
        target_date,
        server_metadata,
        launch_options[:attemps]
      )
    end

    Enum.filter(server_list, fn {atom, _} -> atom == :ok end)
    |> Enum.map(&elem(&1, 1))
    |> Flow.from_enumerable(max_demand: 1, stages: launch_options[:stages])
    |> Flow.map(f)
    |> Enum.to_list()
  end
end
