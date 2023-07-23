defmodule Collector.SMedusaPred do
  @behaviour Collector.Feed

  @table_name :s_medusa_pred

  @enforce_keys [
    :player_id,
    :player_name,
    :player_url,
    :server_id,
    :server_url,
    :alliance_id,
    :alliance_name,
    :alliance_url,
    :inactive_in_future?,
    :inactive_probability,
    :inactive_in_current?,
    :total_population,
    :total_villages,
    :target_date,
    :creation_dt
  ]

  defstruct [
    :player_id,
    :player_name,
    :player_url,
    :server_id,
    :server_url,
    :alliance_id,
    :alliance_name,
    :alliance_url,
    :inactive_in_future?,
    :inactive_probability,
    :inactive_in_current?,
    :total_population,
    :total_villages,
    :target_date,
    :creation_dt
  ]

  @type t :: %__MODULE__{
          player_id: TTypes.player_id(),
          player_name: TTypes.player_name(),
          player_url: binary(),
          server_id: TTypes.server_id(),
          server_url: binary(),
          alliance_id: TTypes.alliance_id(),
          alliance_name: TTypes.alliance_name(),
          alliance_url: binary(),
          inactive_in_future?: boolean(),
          inactive_probability: float(),
          inactive_in_current?: boolean() | :undefined,
          total_population: pos_integer(),
          total_villages: pos_integer(),
          target_date: Date.t(),
          creation_dt: DateTime.t()
        }

  @impl true
  def table_config() do
    options = [
      attributes: [
        :player_id,
        :server_id,
        :target_date,
        :struct
      ],
      type: :set,
      index: [:server_id]
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

  @spec insert_predictions(medusa_structs :: [t()]) :: :ok | {:error, any()}
  def insert_predictions(medusa_structs) do
    func = fn ->
      for x <- medusa_structs,
          do: :mnesia.write({@table_name, x.player_id, x.server_id, x.target_date, x})
    end

    :mnesia.activity(:transaction, func)
  end

  @impl true
  def insert(root_folder, server_id, target_date, _) do
    with(
      {:a, {:ok, s_medusa_pred}} <-
        {:a, Collector.Feed.open(root_folder, server_id, target_date, __MODULE__)},
      func = fn ->
        Enum.map(s_medusa_pred, &insert_record/1)
        :ok
      end,
      {:b, :ok} <- {:b, :mnesia.activity(:transaction, func)}
    ) do
      :ok
    else
      {:a, {:error, reason}} ->
        {:error, {"Unable to open target_date s_medusa_pred", reason}}

      {:b, {:aborted, reason}} ->
        {:error, {"Unable to insert on Mnesia", reason}}
    end
  end

  defp insert_record(x),
    do: :mnesia.write({@table_name, x.player_id, x.server_id, x.target_date, x})

  @impl true
  def options(), do: {Atom.to_string(@table_name), ".c6bert"}

  @impl true
  def to_format(s_medusa_pred),
    do: :erlang.term_to_binary(s_medusa_pred, [:compressed, :deterministic])

  @impl true
  def from_format(encoded_s_medusa_pred),
    do: :erlang.binary_to_term(encoded_s_medusa_pred)

  @impl true
  def run(root_folder, server_id, target_date, _ \\ %{}) do
    with(
      {:a, {:ok, snapshot}} <-
        {:a, Collector.Feed.open(root_folder, server_id, target_date, Collector.Snapshot)},
      {:b, {:ok, agg_players}} <-
        {:b, Collector.Feed.open(root_folder, server_id, target_date, Collector.AggPlayers)},
      {:c, {:ok, medusa_pred_output}} <-
        {:c, Collector.Feed.open(root_folder, server_id, target_date, Collector.MedusaPredOutput)}
    ) do
      player_ids = for row <- medusa_pred_output, do: row.player_id
      uniq_snapshot = Enum.sort_by(snapshot, & &1.player_id) |> Enum.dedup_by(& &1.player_id)

      uniq_agg_players =
        Enum.filter(agg_players, &(&1.player_id in player_ids)) |> Enum.sort_by(& &1.player_id)

      uniq_medusa_pred_output = Enum.sort_by(medusa_pred_output, & &1.player_id)

      bundle = Enum.zip([uniq_snapshot, uniq_agg_players, uniq_medusa_pred_output])
      now = DateTime.utc_now()

      s_medusa_pred =
        for {row, agg_player, medusa_output} <- bundle,
            do: process(target_date, now, server_id, row, agg_player, medusa_output)

      Collector.Feed.store(root_folder, server_id, target_date, s_medusa_pred, __MODULE__)
    else
      {:a, {:error, reason}} ->
        {:error, {"Unable to open target_date snapshot", reason}}

      {:b, {:error, reason}} ->
        {:error, {"Unable to open target_date agg_players", reason}}

      {:c, {:error, reason}} ->
        {:error, {"Unable to open target_date medusa_pred_output", reason}}
    end
  end

  @spec process(
          target_date :: Date.t(),
          creation_dt :: DateTime.t(),
          server_id :: TTypes.server_id(),
          row :: Collector.Snapshot.t(),
          agg_player :: Collector.AggPlayers.t(),
          medusa_output :: Collector.MedusaPredOutput.t()
        ) :: t()
  def process(target_date, creation_dt, server_id, row, agg_player, medusa_output)
      when row.player_id == agg_player.player_id and row.player_id == medusa_output.player_id do
    last_increment = hd(agg_player.increment)

    %__MODULE__{
      player_id: row.player_id,
      player_name: row.player_name,
      player_url: "",
      server_id: server_id,
      server_url: server_id,
      alliance_id: row.alliance_id,
      alliance_name: row.alliance_name,
      alliance_url: "",
      inactive_in_future?: medusa_output.prediction,
      inactive_probability: medusa_output.probability,
      inactive_in_current?: Collector.MedusaTrain.is_inactive?(last_increment),
      total_population: last_increment.total_population,
      total_villages: last_increment.total_villages,
      target_date: target_date,
      creation_dt: creation_dt
    }
  end
end
