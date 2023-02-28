defmodule Front.MedusaController do
  use Front, :controller

  def index(conn, _params) do
    servers = Satellite.ServersTable.get_servers!(Date.utc_today())
    render(conn, "index.html", servers: servers)
  end

  def select(conn, params = %{"server_id" => server_id_path}) do
    s = TTypes.server_id_from_path(server_id_path)
    rows = Satellite.MedusaTable.get_predictions_by_server(s)
    render(conn, "select.html", rows: rows)
  end
end
