defmodule Collector.GenCollector do
  use GenServer
  require Logger

  defstruct [:tref]

  @spec start_link() :: GenServer.on_start()
  def start_link(), do: GenServer.start_link(__MODULE__, [], name: __MODULE__)

  @spec collect() :: :ok
  def collect() do
    send(__MODULE__, :collect)
    :ok
  end

  @impl true
  def init([]) do
    {:ok, %__MODULE__{}, {:continue, :init}}
  end

  @impl true
  def handle_continue(:init, state) do
    collection_hour = Application.fetch_env!(:collector, :collection_hour)
    tref = spawn_next_timer(collection_hour)
    state = Map.put(state, :tref, tref)
    {:noreply, state}
  end

  @impl true
  def handle_call(_msg, _from, state), do: {:noreply, state}

  @impl true
  def handle_cast(_msg, state), do: {:noreply, state}

  @impl true
  def handle_info(:collect, state) do
    Logger.info(%{msg: "Collection started"})

    target_date = Date.utc_today()
    root_folder = Application.fetch_env!(:collector, :root_folder)

    collection_hour = Application.fetch_env!(:collector, :collection_hour)

    launch_options = %{
      min: Application.fetch_env!(:collector, :min),
      max: Application.fetch_env!(:collector, :max),
      attemps: Application.fetch_env!(:collector, :attemps),
      stages: Application.fetch_env!(:collector, :stages)
    }

    retention_period_api_map_sql =
      Application.fetch_env!(:collector, :retention_period_api_map_sql)

    with(
      :ok <- Collector.SServer.clean(target_date, %{}),
      :ok <- Collector.SMedusaPred.clean(target_date, %{}),
      :ok <-
        Collector.ApiMapSql.clean(target_date, %{
          "retention_period" => retention_period_api_map_sql
        }),
      Logger.debug(%{msg: "Mnesia tables cleaned"}),
      {:ok, urls} <- :travianmap.get_urls()
    ) do
      results = Collector.DAG.launch_collection(root_folder, urls, target_date, launch_options)

      log_result = fn {:error, reason} ->
        Logger.warning(%{
          msg: "Unable to collect",
          error: reason
        })
      end

      results
      |> Enum.filter(fn x -> x != :ok end)
      |> Enum.each(fn x -> log_result.(x) end)

      Collector.GenArchive.start_archiving(root_folder, target_date)

      tref = spawn_next_timer(collection_hour)
      state = Map.put(state, :tref, tref)
      Logger.info(%{msg: "Collection finished"})
      {:noreply, state}
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp spawn_next_timer(t) do
    wait_time = Collector.Utils.time_until_hour(t)
    :erlang.send_after(wait_time, Collector.GenCollector, :collect)
  end
end
