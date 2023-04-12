defmodule Collector.AggServer do
  @behaviour Collector.Feed

  @enforce_keys [
    :target_dt,
    :server_id,
    :url,
    :shrink,
    :speed,
    :region,
    :estimated_starting_date,
    :increment
  ]

  defstruct [
    :target_dt,
    :server_id,
    :url,
    :shrink,
    :speed,
    :region,
    :estimated_starting_date,
    :increment
  ]

  @type t :: %__MODULE__{
          target_dt: DateTime.t(),
          server_id: TTypes.server_id(),
          url: String.t(),
          shrink: nil | String.t(),
          speed: nil | pos_integer(),
          region: nil | String.t(),
          estimated_starting_date: Date.t(),
          increment: [Collector.AggServer.Increment.t()]
        }

  @impl true
  def options(), do: {"agg_server", ".c6bert"}

  @impl true
  def to_format(agg_server),
    do: :erlang.term_to_binary(agg_server, [:compressed, :deterministic])

  @impl true
  def from_format(encoded_agg_server),
    do: :erlang.binary_to_term(encoded_agg_server)

  # @spec open(root_folder :: String.t(), server_id :: TTypes.server_id(), target_date :: Date.t()) ::
  #         {:ok, t()} | {:error, any()}
  # def open(root_folder, server_id, target_date) do
  #   case Storage.open(root_folder, server_id, options(), target_date) do
  #     {:ok, {_, encoded}} -> {:ok, from_format(encoded)}
  #     error -> error
  #   end
  # end

  # @spec store(
  #         root_folder :: String.t(),
  #         server_id :: TTypes.server_id(),
  #         agg_server :: t(),
  #         target_date :: Date.t()
  #       ) :: :ok | {:error, any()}
  # def store(root_folder, server_id, agg_server, target_date) do
  #   encoded = to_format(agg_server)
  #   Storage.store(root_folder, server_id, options(), encoded, target_date)
  # end

  @impl true
  @spec run(root_folder :: String.t(), server_id :: TTypes.server_id(), target_date :: Date.t()) ::
          :ok | {:error, any()}
  def run(root_folder, server_id, target_date) do
    with(
      {:a, {:ok, new_snapshot}} <-
        {:a, Collector.Feed.open(root_folder, server_id, target_date, Collector.Snapshot)},
      prev_date = get_prev_date(root_folder, server_id, target_date),
      {:b, {:ok, prev_snapshot}} <-
        {:b,
         open_option(prev_date, fn ->
           Collector.Feed.open(root_folder, server_id, prev_date, Collector.Snapshot)
         end)},
      {:c, {:ok, prev_agg_server}} <-
        {:c,
         open_option(prev_date, fn ->
           Collector.Feed.open(root_folder, server_id, prev_date, __MODULE__)
         end)}
    ) do
      target_dt = DateTime.new!(target_date, ~T[00:00:00.000])

      case prev_date do
        nil ->
          agg_server = process(target_dt, server_id, new_snapshot)
          Collector.Feed.store(root_folder, server_id, target_date, agg_server, __MODULE__)

        _ ->
          agg_server = process(target_dt, server_id, new_snapshot, prev_snapshot, prev_agg_server)
          Collector.Feed.store(root_folder, server_id, target_date, agg_server, __MODULE__)
      end
    else
      {:a, {:error, reason}} -> {:error, {"Unable to open target_date snapshot", reason}}
      {:b, {:error, reason}} -> {:error, {"Unable to open prev_date snapshot", reason}}
      {:c, {:error, reason}} -> {:error, {"Unable to open prev_date agg_server", reason}}
    end
  end

  defp open_option(nil, _open_function), do: {:ok, nil}

  defp open_option(_target_date, open_function) do
    open_function.()
  end

  defp get_prev_date(root_folder, server_id, target_date) do
    case Storage.list_dates(root_folder, server_id, Collector.snapshot_options()) do
      [] ->
        nil

      dates ->
        case dates
             |> Enum.filter(fn d -> Date.compare(target_date, d) == :gt end)
             |> Enum.sort({:desc, Date})
             |> Enum.take(1) do
          [] -> nil
          [prev_date] -> prev_date
        end
    end
  end

  @spec process(
          target_dt :: DateTime.t(),
          server_id :: TTypes.server_id(),
          today_snapshot :: [TTypes.enriched_row()],
          yesterday_snapshot :: [TTypes.enriched_row()],
          yesterday_agg_server :: t()
        ) :: t()
  def process(target_dt, _server_id, new_snapshot, prev_snapshot, prev_agg_server) do
    [prev_increment | _] =
      Enum.sort_by(prev_agg_server.increment, & &1.target_dt, {:desc, DateTime})

    new_increment =
      Collector.AggServer.Increment.increment(
        target_dt,
        new_snapshot,
        prev_snapshot,
        prev_increment
      )

    prev_agg_server
    |> Map.put(:target_dt, target_dt)
    |> Map.update!(:increment, fn inc -> [new_increment | inc] end)
  end

  @spec process(
          target_dt :: DateTime.t(),
          server_id :: TTypes.server_id(),
          new_snapshot :: [Collector.Snapshot.t()]
        ) :: t()
  def process(target_dt, server_id, new_snapshot) do
    {shrink, speed, region} = option_get_metadata_from_url(server_id)

    %__MODULE__{
      target_dt: target_dt,
      server_id: server_id,
      url: server_id,
      shrink: shrink,
      speed: speed,
      region: region,
      estimated_starting_date: DateTime.to_date(target_dt),
      increment: [Collector.AggServer.Increment.increment(target_dt, new_snapshot)]
    }
  end

  defp option_get_metadata_from_url(url) do
    try do
      <<"https://", parsed_url::binary>> = url
      [shrink, <<"x", speed::binary>>, region | _] = String.split(parsed_url, ".")
      {shrink, String.to_integer(speed), region}
    rescue
      _ -> {nil, nil, nil}
    end
  end
end
