defmodule Collector.AggPlayers do
  require Logger

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
          increment: Collector.AggPlayers.Increment.t()
        }

  defp agg_players_options(), do: {"agg_players", ".c6bert"}

  defp agg_players_to_format(agg_players),
    do: :erlang.term_to_binary(agg_players, [:compressed, :deterministic])

  defp agg_players_from_format(encoded_agg_players),
    do: :erlang.binary_to_term(encoded_agg_players)

  @spec open(root_folder :: String.t(), server_id :: TTypes.server_id(), target_date :: Date.t()) ::
          {:ok, [t()]} | {:error, any()}
  def open(root_folder, server_id, target_date) do
    case Storage.open(root_folder, server_id, agg_players_options(), target_date) do
      {:ok, {_, encoded}} -> {:ok, agg_players_from_format(encoded)}
      error -> error
    end
  end

  @spec store(
          root_folder :: String.t(),
          server_id :: TTypes.server_id(),
          agg_players :: [t()],
          target_date :: Date.t()
        ) :: :ok | {:error, any()}
  def store(root_folder, server_id, agg_players, target_date) do
    encoded = agg_players_to_format(agg_players)
    Storage.store(root_folder, server_id, agg_players_options(), encoded, target_date)
  end

  @spec run(root_folder :: String.t(), server_id :: TTypes.server_id(), target_date :: Date.t()) ::
          :ok | {:error, any()}
  def run(root_folder, server_id, target_date) do
    with(
      {:ok, new_snapshot} <- Collector.open(root_folder, server_id, target_date),
      {{:ok, prev_snapshot}, {:ok, prev_agg_players}} <-
        open_option_prev_common_data(root_folder, server_id)
    ) do
      target_dt = DateTime.new!(target_date, ~T[00:00:00.000])

      case {prev_snapshot, prev_agg_players} do
        {nil, nil} ->
          agg_players = process(target_dt, server_id, new_snapshot)
          store(root_folder, server_id, agg_players, target_date)

        _ ->
          agg_players =
            process(target_dt, server_id, new_snapshot, prev_snapshot, prev_agg_players)

          store(root_folder, server_id, agg_players, target_date)
      end
    end
  end

  defp open_option_prev_common_data(root_folder, server_id) do
    case Storage.list_dates(root_folder, server_id, Collector.snapshot_options()) do
      [_target_date] ->
        {{:ok, nil}, {:ok, nil}}

      available_snapshot_dates ->
        [_target_date, prev_date | _] = Enum.sort(available_snapshot_dates, {:desc, Date})

        {Collector.open(root_folder, server_id, prev_date),
         open(root_folder, server_id, prev_date)}
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
    uniq_players = for row <- new_snapshot, uniq: true, do: row.player_id
    find_player = fn player_id -> Enum.find(prev_agg_players, &(&1.player_id == player_id)) end

    for player_id <- uniq_players,
        do:
          group_compute_update(
            target_dt,
            server_id,
            player_id,
            new_snapshot,
            prev_snapshot,
            find_player.(player_id)
          )
  end

  defp group_compute_update(target_dt, server_id, player_id, new_snapshot, _prev_snapshot, nil) do
    rows = Enum.filter(new_snapshot, &(&1.player_id == player_id))
    init_struct(target_dt, server_id, player_id, rows)
  end

  defp group_compute_update(target_dt, _, player_id, new_snapshot, prev_snapshot, prev_agg) do
    {new_player_snapshot, prev_player_snapshot} =
      group_rows_by_player(player_id, new_snapshot, prev_snapshot)

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

  defp group_rows_by_player(player_id, new_snapshot, prev_snapshot) do
    new_village_ids =
      MapSet.new(for row <- new_snapshot, row.player_id == player_id, do: row.village_id)

    prev_village_ids =
      MapSet.new(for row <- prev_snapshot, row.player_id == player_id, do: row.village_id)

    common_villages_ids = MapSet.union(new_village_ids, prev_village_ids) |> MapSet.to_list()

    new_player_rows = for row <- new_snapshot, row.village_id in common_villages_ids, do: row
    prev_player_rows = for row <- prev_snapshot, row.village_id in common_villages_ids, do: row

    {new_player_rows, prev_player_rows}
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
