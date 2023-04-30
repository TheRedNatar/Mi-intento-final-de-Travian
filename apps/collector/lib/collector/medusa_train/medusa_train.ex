defmodule Collector.MedusaTrain do
  @behaviour Collector.Feed

  @enforce_keys [
    :target_dt,
    :server_id,
    :samples
  ]

  defstruct [
    :target_dt,
    :server_id,
    :samples
  ]

  @type t :: %__MODULE__{
          target_dt: DateTime.t(),
          server_id: TTypes.server_id(),
          samples: [Collector.MedusaTrain.Sample.t()]
        }

  # @spec is_inactive?(inc :: Collector.AggPlayers.Increment.t()) :: nil | boolean()
  # def is_inactive?(inc) do
  #   case {inc.population_increase, inc.population_increase_by_founded,
  #         inc.population_increase_by_conquered} do
  #     {nil, nil, nil} -> nil
  #     {vill_inc, founded, conquered} -> (vill_inc + founded + conquered) == 0
  #   end
  # end

  @spec is_inactive?(inc :: Collector.AggPlayers.Increment.t()) :: boolean()
  def is_inactive?(inc) do
    inc.population_increase + inc.population_increase_by_founded +
      inc.population_increase_by_conquered == 0
  end

  @impl true
  def options(), do: {"medusa_train", ".c6bert"}

  @impl true
  def to_format(medusa_train),
    do: :erlang.term_to_binary(medusa_train, [:compressed, :deterministic])

  @impl true
  def from_format(encoded_medusa_train),
    do: :erlang.binary_to_term(encoded_medusa_train)

  @impl true
  def run(root_folder, server_id, target_date) do
    with(
      {:a, {:ok, agg_players}} <-
        {:a, Collector.Feed.open(root_folder, server_id, target_date, Collector.AggPlayers)},
      {:b, {:ok, medusa_pred_input}} <-
        {:b,
         Collector.Feed.open(
           root_folder,
           server_id,
           Date.add(target_date, -1),
           Collector.MedusaPredInput
         )}
    ) do
      player_ids = for row <- medusa_pred_input, do: row.player_id
      uniq_agg_players = for agg_p <- agg_players, agg_p.player_id in player_ids, do: agg_p

      # We should have the same players in both feeds
      bundle =
        Enum.zip(
          Enum.sort_by(medusa_pred_input, & &1.player_id),
          Enum.sort_by(uniq_agg_players, & &1.player_id)
        )

      target_dt = DateTime.new!(target_date, ~T[00:00:00.000])

      samples =
        for {input, agg_p} <- bundle,
            agg_p.target_dt == target_dt,
            do: process(target_dt, agg_p, input)

      medusa_train = %__MODULE__{
        target_dt: target_dt,
        server_id: server_id,
        samples: samples
      }

      Collector.Feed.store(root_folder, server_id, target_date, medusa_train, __MODULE__)
    else
      {:a, {:error, reason}} -> {:error, {"Unable to open agg_players", reason}}
      {:b, {:error, reason}} -> :ok
    end
  end

  @spec process(
          target_dt :: DateTime.t(),
          agg_player :: Collector.AggPlayers.t(),
          input :: Collector.MedusaPredInput.t()
        ) :: Collector.MedusaTrain.Sample.t()
  def process(target_dt, agg_player, input)
      when agg_player.player_id == input.player_id do
    %Collector.MedusaTrain.Sample{
      sample: input,
      labeling_dt: target_dt,
      is_inactive?: is_inactive?(Enum.find(agg_player.increment, &(&1.target_dt == target_dt)))
    }
  end
end
