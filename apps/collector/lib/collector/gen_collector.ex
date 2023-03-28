defmodule Collector.GenCollector do
  use GenServer
  require Logger

  defstruct [:tref, active_p: %{}, target_date: nil, subscriptions: []]

  @spec start_link() :: GenServer.on_start()
  def start_link(), do: GenServer.start_link(__MODULE__, [], name: __MODULE__)

  @spec collect() :: :ok
  def collect() do
    send(__MODULE__, :collect)
    :ok
  end

  @spec subscribe() :: reference()
  def subscribe() do
    :ok = GenServer.call(__MODULE__, :subscribe)
    ref = Process.monitor(Collector.GenCollector)
    ref
  end

  @impl true
  def init([]) do
    {:ok, %__MODULE__{}, {:continue, :init}}
  end

  @impl true

  def handle_continue(:init, state) do
    collection_hour = Application.fetch_env!(:collector, :collection_hour)
    wait_time = Collector.Utils.time_until_collection(collection_hour)
    tref = :erlang.send_after(wait_time, self(), :collect)
    state = Map.put(state, :tref, tref)
    {:noreply, state}
  end

  @impl true
  def handle_continue(:is_finished, state = %__MODULE__{active_p: active_p})
      when map_size(active_p) == 0 do
    Enum.each(state.subscriptions, fn x -> send(x, {:collector_event, :collection_finished}) end)
    collection_hour = Application.fetch_env!(:collector, :collection_hour)
    wait_time = Collector.Utils.time_until_collection(collection_hour)
    tref = :erlang.send_after(wait_time, self(), :collect)
    state = Map.put(state, :tref, tref)
    Logger.info(%{msg: "Collection finished"})
    {:noreply, state}
  end

  def handle_continue(:is_finished, state), do: {:noreply, state}

  @impl true
  def handle_call(:subscribe, {pid, _}, state = %__MODULE__{subscriptions: s}) do
    new_s = Enum.uniq([pid | s])
    new_state = Map.put(state, :subscriptions, new_s)
    {:reply, :ok, new_state}
  end

  def handle_call(_msg, _from, state), do: {:noreply, state}

  @impl true
  def handle_cast({result, server_id, pid}, state = %__MODULE__{active_p: ap})
      when is_map_key(ap, pid) do
    {{ref, _}, new_ap} = Map.pop!(ap, pid)
    Process.demonitor(ref, [:flush])
    new_state = Map.put(state, :active_p, new_ap)

    Enum.each(state.subscriptions, fn x -> send(x, {:collector_event, {result, server_id}}) end)

    Logger.info(%{
      msg: "Collection finished for #{server_id} at #{state.target_date}",
      server_id: server_id,
      target_date: state.target_date,
      result: result
    })

    {:noreply, new_state, {:continue, :is_finished}}
  end

  def handle_cast(_msg, state), do: {:noreply, state}

  @impl true
  def handle_info(:collect, state) do
    Logger.info(%{msg: "Collection started"})

    case :travianmap.get_urls() do
      {:error, reason} ->
        Logger.error(%{msg: "Unable to start the colletion", reason: reason})
        tref = :erlang.send_after(3_000, self(), :collect)
        new_state = Map.put(state, :tref, tref)
        {:noreply, new_state}

      {:ok, urls} ->
        Enum.each(state.subscriptions, fn x ->
          send(x, {:collector_event, :collection_started})
        end)

        root_folder = Application.fetch_env!(:collector, :root_folder)
        target_date = Date.utc_today()

        {childs, errors} =
          Enum.map(urls, fn server_id ->
            Collector.Supervisor.Worker.start_child(root_folder, server_id, target_date)
          end)
          |> Enum.split_with(fn {atom, _} -> atom == :ok end)

        Enum.each(
          errors,
          &Logger.warning(%{msg: "Unable to start Collector.GenWorker", reason: elem(&1, 1)})
        )

        ap = for {:ok, {pid, ref, server_id}} <- childs, into: %{}, do: {pid, {ref, server_id}}

        collection_hour = Application.fetch_env!(:collector, :collection_hour)
        wait_time = Collector.Utils.time_until_collection(collection_hour)
        tref = :erlang.send_after(wait_time, self(), :collect)

        new_state =
          state
          |> Map.put(:active_p, ap)
          |> Map.put(:tref, tref)
          |> Map.put(:target_date, target_date)

        {:noreply, new_state, {:continue, :is_finished}}
    end
  end

  def handle_info(
        {:DOWN, _ref, :process, pid, _reason},
        state = %__MODULE__{active_p: ap, target_date: target_date}
      )
      when is_map_key(ap, pid) do
    {{_old_ref, server_id}, new_ap} = Map.pop!(ap, pid)

    root_folder = Application.fetch_env!(:collector, :root_folder)

    case Collector.Supervisor.Worker.start_child(root_folder, server_id, target_date) do
      {:ok, {pid, ref}} ->
        new_ap = Map.put(new_ap, pid, {ref, server_id})
        new_state = Map.put(state, :active_p, new_ap)
        {:noreply, new_state, {:continue, :is_finished}}

      {:error, reason} ->
        Logger.error(%{
          msg: "Unable to relaunch #{server_id}",
          reason: reason,
          server_id: server_id
        })

        new_state = Map.put(state, :active_p, new_ap)
        {:noreply, new_state, {:continue, :is_finished}}
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}
end
