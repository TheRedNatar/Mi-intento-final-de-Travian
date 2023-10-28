defmodule Collector.ApiMapSql.EnrichedRow do
  @derive Jason.Encoder

  @enforce_keys [
    :map_id,
    :x,
    :y,
    :tribe,
    :village_id,
    :village_name,
    :player_id,
    :player_name,
    :alliance_id,
    :alliance_name,
    :population,
    :region,
    :is_capital,
    :is_city,
    :has_harbor,
    :victory_points,
    :player_played_yesterday?,
    :player_will_play_today_prediction?,
    :prediction_confidence
  ]

  defstruct [
    :map_id,
    :x,
    :y,
    :tribe,
    :village_id,
    :village_name,
    :player_id,
    :player_name,
    :alliance_id,
    :alliance_name,
    :population,
    :region,
    :is_capital,
    :is_city,
    :has_harbor,
    :victory_points,
    :player_played_yesterday?,
    :player_will_play_today_prediction?,
    :prediction_confidence
  ]

  @type t :: %__MODULE__{
          map_id: TTypes.map_id(),
          x: TTypes.x(),
          y: TTypes.y(),
          tribe: TTypes.tribe_integer(),
          village_id: TTypes.village_id(),
          village_name: TTypes.village_name(),
          player_id: TTypes.player_id(),
          player_name: TTypes.player_name(),
          alliance_id: TTypes.alliance_id(),
          alliance_name: TTypes.alliance_name(),
          population: TTypes.population(),
          region: TTypes.region(),
          is_capital: TTypes.is_capital(),
          is_city: TTypes.is_city(),
          has_harbor: TTypes.has_harbor(),
          victory_points: TTypes.victory_points(),
          player_played_yesterday?: :undefined | boolean(),
          player_will_play_today_prediction?: boolean(),
          prediction_confidence: float()
        }
end
