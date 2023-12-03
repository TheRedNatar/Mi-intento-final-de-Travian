defmodule Front.Api.ServerController do
  use Front, :controller

  # {
  # @api {get} /api/servers Request available servers
  # @apiName GetServers
  # @apiGroup Servers
  #
  #
  # @apiSuccess {String[]} data List of available servers to use as :server_id parameter.
  # @apiSuccessExample {json} Success-Response:
  # HTTP/1.1 200 OK
  # {
  # "data":[
  #     "alpler.x1.tr.travian.com",
  #     "beta.travian.com",
  #     "cw.x1.international.travian.com"]}
  # }
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
