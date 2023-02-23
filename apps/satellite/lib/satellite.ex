defmodule Satellite do
  @moduledoc """
  Documentation for `Satellite`.
  """

  @spec install(nodes :: [atom()]) :: :ok | {:error, any()}
  def install(nodes) do
      :rpc.multicall(nodes, :application, :stop, [:mnesia])
    with(
      {:step_1, :ok} <- {:step_1, :mnesia.delete_schema(nodes)},
      {:step_2, :ok} <- {:step_2, :mnesia.create_schema(nodes)},
      :rpc.multicall(nodes, :application, :start, [:mnesia]),
      {:step_3, {:atomic, _res}} <- {:step_3, Satellite.MedusaTable.create_table(nodes)},
      {:step_4, {:atomic, _res}} <- {:step_4, Satellite.ServersTable.create_table(nodes)}
    ) do
      :ok
    else
      reason -> {:error, reason}
    end
  end
end
