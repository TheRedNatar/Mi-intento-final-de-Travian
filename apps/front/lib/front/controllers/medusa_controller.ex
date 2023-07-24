defmodule Front.MedusaController do
  use Front, :controller

  def index(conn, _params) do
    servers = get_servers!()
    render(conn, "index.html", servers: servers)
  end

  def select(conn, _params = %{"server_id" => server_id_path}) do
    server_id = TTypes.server_id_from_path(server_id_path)
    rows = get_predictions(server_id)
    render(conn, "select.html", rows: rows)
  end

  def get_predictions(server_id) do
    {table_name, _} = Collector.SMedusaPred.options()
    pattern = {String.to_atom(table_name), :_, server_id, :_, :_}
    func = fn -> :mnesia.match_object(pattern) end
    answer = :mnesia.activity(:transaction, func)

    for {_table_name, _player_id, _server_id, _target_date_row, row} <- answer, do: row
  end

  def get_servers!() do
    {table_name, _} = Collector.SServer.options()
    func = fn -> :mnesia.all_keys(String.to_atom(table_name)) end
    :mnesia.activity(:transaction, func)
  end
end
