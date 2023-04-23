defmodule Collector.MedusaTrain do
  @behaviour Collector.Feed

  @spec is_inactive?(inc :: Collector.AggPlayers.Increment.t()) :: nil | boolean()
  def is_inactive?(inc) do
    case {inc.population_increase, inc.population_increase_by_founded,
          inc.population_increase_by_conquered} do
      {nil, nil, nil} -> nil
      {vill_inc, founded, conquered} -> vill_inc + founded + conquered == 0
    end
  end
end
