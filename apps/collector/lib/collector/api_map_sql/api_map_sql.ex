defmodule Collector.ApiMapSql do
  @behaviour Collector.Feed

  @enforce_keys [
    :server_id,
    :target_date,
    :rows
  ]

  defstruct [
    :server_id,
    :target_date,
    :rows
  ]

  @type t :: %__MODULE__{
          server_id: TTypes.server_id(),
          target_date: Date.t(),
          rows: [Collector.ApiMapSql.EnrichedRow.t()]
        }

  @table_name :api_map_sql

  @impl true
  def table_config() do
    options = [
      attributes: [
        :target_date_gregorian,
        :server_id,
        :json
      ],
      type: :bag
    ]

    {@table_name, options}
  end

  @impl true
  def clean(target_date, options) when is_map_key(options, "retention_period") do
    # oldest_date =
    #   Date.add(target_date, -Map.fetch!(options, "retention_period")) |> Date.to_gregorian_days()

    # match_head = {:_, :"$1", :_, :_}
    # guard = [{:<, :"$1", oldest_date}]
    # result = [:"$1"]
    # match_specification = [{match_head, guard, result}]

    # func = fn ->
    #   :mnesia.select(@table_name, match_specification)
    #   |> Enum.each(fn date_to_delete -> :mnesia.delete({@table_name, date_to_delete}) end)
    # end

    # case :mnesia.activity(:sync_transaction, func) do
    #   :ok -> :ok
    #   {:atomic, :ok} -> :ok
    #   error = {:aborted, _reason} -> {:error, error}
    # end

    case :mnesia.clear_table(@table_name) do
      {:atomic, :ok} -> :ok
      error = {:aborted, _reason} -> {:error, error}
    end

  end

  @impl true
  def insert(root_folder, server_id, target_date, _) do
    with(
      {:a, {:ok, api_map_sql}} <-
        {:a, Collector.Feed.open(root_folder, server_id, target_date, __MODULE__)},
      func = fn ->
        :mnesia.write(
          {@table_name, Date.to_gregorian_days(api_map_sql.target_date), api_map_sql.server_id,
           to_json(api_map_sql.rows)}
        )
      end,
      {:b, :ok} <- {:b, :mnesia.activity(:sync_transaction, func)}
    ) do
      :ok
    else
      {:a, {:error, reason}} ->
        {:error, {"Unable to open target_date api_map_sql", reason}}

      {:b, {:aborted, reason}} ->
        {:error, {"Unable to insert in Mnesia", reason}}
    end
  end

  @impl true
  def options(), do: {"api_map_sql", ".c6bert"}

  @impl true
  def to_format(api_map_sql),
    do: :erlang.term_to_binary(api_map_sql, [:compressed, :deterministic])

  @impl true
  def from_format(encoded), do: :erlang.binary_to_term(encoded)

  @impl true
  def run(root_folder, server_id, target_date, _ \\ %{}) do
    with(
      {:a, {:ok, snapshot}} <-
        {:a, Collector.Feed.open(root_folder, server_id, target_date, Collector.Snapshot)},
      {:b, {:ok, agg_players}} <-
        {:b, Collector.Feed.open(root_folder, server_id, target_date, Collector.AggPlayers)},
      {:c, {:ok, medusa_pred_output}} <-
        {:c, Collector.Feed.open(root_folder, server_id, target_date, Collector.MedusaPredOutput)},
      players_map = create_players_map(agg_players, medusa_pred_output),
      enriched_map_sql_rows = process_rows(snapshot, players_map),
      struct = %__MODULE__{
        server_id: server_id,
        target_date: target_date,
        rows: enriched_map_sql_rows
      },
      {:d, :ok} <-
        {:d, Collector.Feed.store(root_folder, server_id, target_date, struct, __MODULE__)}
    ) do
      :ok
    else
      {:a, {:error, reason}} -> {:error, {"Unable to open snapshot", reason}}
      {:b, {:error, reason}} -> {:error, {"Unable to open agg_players", reason}}
      {:c, {:error, reason}} -> {:error, {"Unable to open medusa_pred_output", reason}}
      {:d, {:error, reason}} -> {:error, {"Unable to store api_map_sql", reason}}
    end
  end

  defp create_players_map(agg_players, medusa_pred_output) do
    sorted_agg_players = Enum.sort_by(agg_players, & &1.player_id)
    sorted_medusa_pred_output = Enum.sort_by(medusa_pred_output, & &1.player_id)
    player_ids = Enum.map(sorted_medusa_pred_output, & &1.player_id)

    Enum.zip([player_ids, sorted_agg_players, sorted_medusa_pred_output])
    |> Map.new(fn {player_id, agg_player, medusa_pred_o} ->
      {player_id, {agg_player, medusa_pred_o}}
    end)
  end

  defp process_rows(snapshot, players_map) do
    for row <- snapshot, do: process_row(row, players_map)
  end

  defp process_row(row, players_map) do
    {agg_player, medusa_pred_o} = Map.get(players_map, row.player_id)

    [last | _rest] = Enum.sort_by(agg_player.increment, & &1.target_dt, {:desc, Date})

    %Collector.ApiMapSql.EnrichedRow{
      map_id: row.map_id,
      x: row.x,
      y: row.y,
      tribe: TTypes.decode_tribe(row.tribe),
      village_id: row.village_server_id,
      village_name: row.village_name,
      player_id: row.player_server_id,
      player_name: row.player_name,
      alliance_id: row.alliance_server_id,
      alliance_name: row.alliance_name,
      population: row.population,
      region: row.region,
      is_capital: row.is_capital,
      is_city: row.is_city,
      has_harbor: row.has_harbor,
      victory_points: row.victory_points,
      player_played_yesterday?: player_played_yesterday?(last),
      player_will_play_today_prediction?: medusa_pred_o.prediction,
      prediction_confidence: Float.round(2 * abs(medusa_pred_o.probability - 0.5), 5)
    }
  end

  defp player_played_yesterday?(%{population_increase: population_increase})
       when population_increase == nil do
    :undefined
  end

  defp player_played_yesterday?(%{population_increase: population_increase})
       when population_increase == 0 do
    false
  end

  defp player_played_yesterday?(%{population_increase: population_increase})
       when population_increase > 0 do
    true
  end

  defp to_json(enriched_rows) do
    case Jason.encode(enriched_rows) do
      {:ok, json} -> {:ok, Jason.Formatter.minimize(json)}
      error -> error
    end
  end
end
