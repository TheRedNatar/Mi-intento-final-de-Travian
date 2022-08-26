defmodule Medusa do
  @moduledoc """
  `Medusa` is the application which holds the pipeline for making predictions.
  """

  @type player_status :: :active, :inactive, :future_inactive


  @spec subscribe() :: :ok
  def subscribe(), do: :ok

  @spec unsubscribe() :: :ok
  def unsubscribe(), do: :ok

  @spec etl(root_folder :: binary(), port :: pid(), server_id :: TTypes.server_id(), target_date :: Date.t()) :: {:ok, [map()]} | {:error, any()}
  def etl(root_folder, port, server_id, target_date \\ Date.utc_today()) when is_binary(root_folder) and is_pid(port) and is_binary(server_id) do
    Medusa.ETL.apply(root_folder, port, server_id, target_date)
  end


  @spec predictions_to_format(predictions :: [map()]) :: binary()
  def predictions_to_format(predictions),
    do: :erlang.term_to_binary(predictions, [:compressed, :deterministic])

  @spec predictions_from_format(encoded_predictions :: binary()) :: [map()]
  def predictions_from_format(encoded_predictions), do: :erlang.binary_to_term(encoded_predictions)
  

end
