defmodule Collector.AggPlayers do
  @enforce_keys [
    :target_date,
    :server_id,
    :player_id,
    :estimated_starting_date,
    :estimated_tribe,
    :increment
  ]

  defstruct [
    :target_date,
    :server_id,
    :player_id,
    :estimated_starting_date,
    :estimated_tribe,
    :increment
  ]

  @type t :: %__MODULE__{
          target_date: Date.t(),
          server_id: TTypes.server_id(),
          player_id: TTypes.player_id(),
          estimated_starting_date: Date.t(),
          estimated_tribe: TTypes.tribe_integer(),
          increment: Collector.AggPlayers.Increment.t()
        }

  @spec run(root_folder :: String.t(), server_id :: TTypes.server_id(), target_date :: Date.t()) ::
          :ok | {:error, any()}
  def run(root_folder, server_id, target_date) do
  end

  @spec process(
          today_snapshot :: [TTypes.enriched_row()],
          yesterday_snapshot :: [TTypes.enriched_row()] | nil,
          yesterday_agg_players :: [t()] | nil
        ) :: [t()]
  def process(today_snapshot, yesterday_snapshot, yesterday_agg_players)
  def process(today_snapshot, nil, nil), do: :ok
  def process(today_snapshot, yesterday_snapshot, yesterday_agg_players), do: :ok

  @spec increment(
          today_player_snapshot :: [TTypes.enriched_row()],
          yesterday_player_snapshot :: [TTypes.enriched_row()] | nil,
          agg_player :: t() | nil
        ) :: t()
  def increment(today_player_snapshot, yesterday_player_snapshot, agg_player), do: :ok
end
