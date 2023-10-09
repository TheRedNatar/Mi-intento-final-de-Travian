defmodule Collector.MedusaScore do
  @behaviour Collector.Feed

  @type score :: :true_positive | :true_negative | :false_positive | :false_negative

  @enforce_keys [
    :target_dt,
    :player_id,
    :score,
    :probability
  ]

  @derive Jason.Encoder
  defstruct [
    :target_dt,
    :player_id,
    :score,
    :probability
  ]

  @type t :: %__MODULE__{
          target_dt: DateTime.t(),
          player_id: TTypes.player_id(),
          score: score(),
          probability: float()
        }

  @impl true
  def options(), do: {"medusa_score", ".c6bert"}

  @impl true
  def to_format(medusa_score),
    do: :erlang.term_to_binary(medusa_score, [:compressed, :deterministic])

  @impl true
  def from_format(encoded_medusa_score),
    do: :erlang.binary_to_term(encoded_medusa_score)

  @impl true
  def run(root_folder, server_id, target_date, _ \\ %{}) do
    with(
      {:a, {:ok, agg_players}} <-
        {:a, Collector.Feed.open(root_folder, server_id, target_date, Collector.AggPlayers)},
      {:b, {:ok, medusa_pred_output}} <-
        {:b,
         Collector.Feed.open(
           root_folder,
           server_id,
           Date.add(target_date, -1),
           Collector.MedusaPredOutput
         )},
      target_dt = DateTime.new!(target_date, ~T[00:00:00.000]),
      current_agg_player =
        Enum.filter(agg_players, &(DateTime.compare(&1.target_dt, target_dt) == :eq))
        |> MapSet.new(),
      agg_player_ids = Enum.map(current_agg_player, & &1.player_id) |> MapSet.new(),
      pred_player_ids = Enum.map(medusa_pred_output, & &1.player_id) |> MapSet.new(),
      common_player_ids =
        MapSet.intersection(agg_player_ids, pred_player_ids) |> MapSet.to_list(),
      common_agg_players =
        Enum.filter(current_agg_player, &(&1.player_id in common_player_ids))
        |> Enum.sort_by(& &1.player_id),
      common_pred_output =
        Enum.filter(medusa_pred_output, &(&1.player_id in common_player_ids))
        |> Enum.sort_by(& &1.player_id),
      medusa_score =
        Enum.map(Enum.zip(common_agg_players, common_pred_output), fn {agg, out} ->
          create_score(target_dt, agg, out)
        end),
      {:c, Collector.Feed.store(root_folder, server_id, target_date, medusa_score, __MODULE__)}
    ) do
      :ok
    else
      {:a, {:error, reason}} ->
        {:error, {"Unable to open agg_players", reason}}

      # Unable to open prev file, but we shouldnt fail here
      {:b, {:error, _reason}} ->
        :ok

      {:c, {:error, reason}} ->
        {:error, {"Unable to store medusa_score", reason}}
    end
  end

  defp create_score(target_dt, agg_player, pred_output)
       when agg_player.player_id == pred_output.player_id do
    actual_activity = Collector.MedusaTrain.is_inactive?(hd(agg_player.increment))

    %__MODULE__{
      target_dt: target_dt,
      player_id: agg_player.player_id,
      score: assign_score(actual_activity, pred_output.prediction),
      probability: pred_output.probability
    }
  end

  @spec assign_score(actual_activity :: boolean() | :undefined, estimated_activity :: boolean()) ::
          score()
  def assign_score(true, true), do: :true_positive
  def assign_score(false, false), do: :true_negative
  def assign_score(true, false), do: :false_negative
  def assign_score(false, true), do: :false_positive

  def assign_score(:undefined, _),
    do: raise(ArgumentError, message: ":undefined player with 2 days of activity")
end
