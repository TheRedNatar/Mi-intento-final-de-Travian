defmodule Satellite.ServersTable do
  require Record

  @table_name :servers_table

  @enforce_keys [
    :server_id,
    :modification_dt
  ]

  defstruct [
    :server_id,
    :modification_dt
  ]

  @type t :: %__MODULE__{
          server_id: TTypes.server_id(),
	  modification_dt: DateTime.t()
        }

  @spec create_table(nodes :: [atom()]) :: {:atomic, any()} | {:aborted, any()}
  def create_table(nodes) do
    options = [
      attributes: [
        :server_id,
        :modification_dt
      ],
      type: :set,
      disc_copies: nodes
    ]

    :mnesia.create_table(@table_name, options)
  end

  @spec upsert_server!(server_id :: TTypes.server_id()) :: :ok
  def upsert_server!(server_id) do

    now = DateTime.now!("Etc/UTC")
    f = fn -> :mnesia.write({@table_name, server_id, now}) end
    :mnesia.activity({:transaction, 10}, f)
  end

  @spec get_servers!(target_date :: Date.t()) :: [TTypes.server_id()]
  def get_servers!(target_date \\ Date.utc_today()) do

    all = {@table_name, :_, :_}
    f = fn -> :mnesia.match_object(all) end
    servers = :mnesia.activity({:transaction, 10}, f)
    filter_servers(servers, target_date)
  end

  defp same_date(date1, date2), do: Date.compare(date1, date2) == :eq
  defp filter_servers(tuples, target_date) do
    for {@table_name, server_id, modification_dt} <- tuples, same_date(DateTime.to_date(modification_dt), target_date), do: server_id
  end
  end



