defmodule Front.Api.ServerController do
  use Front, :controller

  def index(conn, _params) do
    servers = get_servers_keys!()
    render(conn, :index, servers: servers)
  end

  def get_servers_keys!() do
    func = fn -> :mnesia.all_keys(:s_server) end

    :mnesia.activity(:transaction, func)
    |> Enum.sort()
  end
end
