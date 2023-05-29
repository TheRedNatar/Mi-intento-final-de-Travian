defmodule Collector.AggPlayers do
  @behaviour Collector.Feed

  @enforce_keys [
    :target_dt,
    :server_id,
    :player_id,
    :estimated_starting_date,
    :estimated_tribe,
    :increment
  ]

  defstruct [
    :target_dt,
    :server_id,
    :player_id,
    :estimated_starting_date,
    :estimated_tribe,
    :increment
  ]

  @type t :: %__MODULE__{
          target_dt: DateTime.t(),
          server_id: TTypes.server_id(),
          player_id: TTypes.player_id(),
          estimated_starting_date: Date.t(),
          estimated_tribe: TTypes.tribe_integer(),
          increment: [Collector.AggPlayers.Increment.t()]
        }

  @impl true
  def options(), do: {"agg_players", ".c6bert"}

  @impl true
  def to_format(agg_players),
    do: :erlang.term_to_binary(agg_players, [:compressed, :deterministic])

  @impl true
  def from_format(encoded_agg_players),
    do: :erlang.binary_to_term(encoded_agg_players)

  @impl true
  def run(root_folder, server_id, target_date, _ \\ %{}) do
    with(
      {:a, {:ok, new_snapshot}} <-
        {:a, Collector.Feed.open(root_folder, server_id, target_date, Collector.Snapshot)},
      prev_date = get_prev_date(root_folder, server_id, target_date),
      {:b, {:ok, prev_snapshot}} <-
        {:b,
         open_option(prev_date, fn ->
           Collector.Feed.open(root_folder, server_id, prev_date, Collector.Snapshot)
         end)},
      {:c, {:ok, prev_agg_players}} <-
        {:c,
         open_option(prev_date, fn ->
           Collector.Feed.open(root_folder, server_id, prev_date, __MODULE__)
         end)}
    ) do
      target_dt = DateTime.new!(target_date, ~T[00:00:00.000])

      case prev_date do
        nil ->
          agg_players = process(target_dt, server_id, new_snapshot)
          Collector.Feed.store(root_folder, server_id, target_date, agg_players, __MODULE__)

        _ ->
          agg_players =
            process(target_dt, server_id, new_snapshot, prev_snapshot, prev_agg_players)

          Collector.Feed.store(root_folder, server_id, target_date, agg_players, __MODULE__)
      end
    else
      {:a, {:error, reason}} -> {:error, {"Unable to open target_date snapshot", reason}}
      {:b, {:error, reason}} -> {:error, {"Unable to open prev_date snapshot", reason}}
      {:c, {:error, reason}} -> {:error, {"Unable to open prev_date agg_players", reason}}
    end
  end

  defp open_option(nil, _open_function), do: {:ok, nil}

  defp open_option(_target_date, open_function) do
    open_function.()
  end

  defp get_prev_date(root_folder, server_id, target_date) do
    case Storage.list_dates(root_folder, server_id, Collector.Snapshot.options()) do
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
          yesterday_agg_players :: [t()]
        ) :: [t()]
  def process(target_dt, server_id, new_snapshot, prev_snapshot, prev_agg_players) do
    new_players = MapSet.new(Enum.map(new_snapshot, & &1.player_id) |> Enum.uniq())
    prev_players = MapSet.new(Enum.map(prev_snapshot, & &1.player_id) |> Enum.uniq())
    abandoned_players = MapSet.difference(prev_players, new_players) |> MapSet.to_list()

    player_villages = common_villages(new_snapshot, prev_snapshot)
    map_agg_players = Map.new(prev_agg_players, fn x -> {x.player_id, x} end)

    new_increments =
      for player_id <- new_players,
          do:
            compute_update(
              target_dt,
              server_id,
              player_id,
              new_snapshot,
              prev_snapshot,
              Map.get(map_agg_players, player_id),
              Map.fetch!(player_villages, player_id)
            )

    prev_increments =
      for x <- prev_agg_players,
          x.player_id in abandoned_players,
          do: x

    new_increments ++ prev_increments
  end

  @spec common_villages(
          new_snapshot :: [Collector.Snapshot.t()],
          prev_snapshot :: [Collector.Snapshot.t()]
        ) :: %{TTypes.player_id() => [TTypes.village_id(), ...]}
  def common_villages(new_snapshot, prev_snapshot) do
    (new_snapshot ++ prev_snapshot)
    |> Enum.group_by(& &1.player_id, & &1.village_id)
    |> Enum.map(fn {player_id, village_ids} -> {player_id, Enum.uniq(village_ids)} end)
    |> Map.new()
  end

  defp compute_update(
         target_dt,
         server_id,
         player_id,
         new_snapshot,
         _prev_snapshot,
         nil,
         _village_ids
       ) do
    rows = Enum.filter(new_snapshot, &(&1.player_id == player_id))
    init_struct(target_dt, server_id, player_id, rows)
  end

  defp compute_update(target_dt, _, player_id, new_snapshot, prev_snapshot, prev_agg, village_ids) do
    new_player_snapshot = Enum.filter(new_snapshot, &(&1.village_id in village_ids))
    prev_player_snapshot = Enum.filter(prev_snapshot, &(&1.village_id in village_ids))

    new_increment =
      Collector.AggPlayers.Increment.increment(
        target_dt,
        player_id,
        new_player_snapshot,
        prev_player_snapshot
      )

    prev_agg
    |> Map.put(:target_dt, target_dt)
    |> Map.update!(:increment, fn list_of_increments -> [new_increment | list_of_increments] end)
  end

  @spec process(
          target_dt :: DateTime.t(),
          server_id :: TTypes.server_id(),
          new_snapshot :: [TTypes.enriched_row()]
        ) :: [t()]
  def process(target_dt, server_id, new_snapshot) do
    new_snapshot
    |> Enum.group_by(& &1.player_id)
    |> Enum.map(fn {player_id, rows} -> init_struct(target_dt, server_id, player_id, rows) end)
  end

  defp init_struct(target_dt, server_id, player_id, rows) do
    %__MODULE__{
      target_dt: target_dt,
      server_id: server_id,
      player_id: player_id,
      estimated_starting_date: DateTime.to_date(target_dt),
      estimated_tribe: get_max_tribe(rows),
      increment: [Collector.AggPlayers.Increment.increment(target_dt, rows)]
    }
  end

  defp get_max_tribe(rows) do
    Enum.map(rows, & &1.tribe)
    |> Enum.frequencies()
    |> Enum.max_by(fn {_tribe, freq} -> freq end)
    |> elem(0)
  end
end
