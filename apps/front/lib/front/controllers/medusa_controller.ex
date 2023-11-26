defmodule Front.MedusaController do
  use Front, :controller

  def index(conn, _params) do
    case get_servers!() do
      [] -> render(conn, "index.html", servers: [], last_update: Date.utc_today())
      servers -> 
	last_update = Front.Utils.get_target_date!(hd(servers))
	render(conn, "index.html", servers: servers, last_update: last_update)
    end
  end

  def select(
        conn,
        _params = %{"server_id" => server_id_path, "position_x" => x, "position_y" => y}
      ) do
    server_id = TTypes.server_id_from_path(server_id_path)
    rows = get_predictions(server_id)
    fixed_x = fix_position_parameter(x)
    fixed_y = fix_position_parameter(y)
    render(conn, "select.html", rows: rows, position_x: fixed_x, position_y: fixed_y)
  end

  def select(conn, _params = %{"server_id" => server_id_path}) do
    server_id = TTypes.server_id_from_path(server_id_path)
    rows = get_predictions(server_id)
    render(conn, "select.html", rows: rows, position_x: "0", position_y: "0")
  end

  def get_predictions(server_id) do
    pattern = {:s_medusa_pred, :_, server_id, :_, :_}
    func = fn -> :mnesia.match_object(pattern) end
    answer = :mnesia.activity(:transaction, func)

    for {_table_name, _player_id, _server_id, _target_date_row, row} <- answer, do: row
  end

  def get_servers!() do
    func = fn -> :mnesia.all_keys(:s_server) end

    :mnesia.activity(:transaction, func)
    |> Enum.sort()
  end

  def get_target_date!(server_id) do
    pattern = {:s_server, server_id, :_, :_}
    func = fn -> :mnesia.match_object(pattern) end
    [{_, _, date, _}] = :mnesia.activity(:transaction, func)
    date
  end

  defp fix_position_parameter(x) do
    try do
      case String.to_integer(x) do
        n when n > 200 -> "200"
        n when n < -200 -> "200"
        _ -> x
      end
    rescue
      _ -> "0"
    end
  end
end
