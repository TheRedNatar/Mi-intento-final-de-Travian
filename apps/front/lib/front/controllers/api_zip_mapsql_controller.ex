defmodule Front.Api.Zip.MapSQLController do
  use Front, :controller

  def show(conn, %{"server_id" => server_id_without_https, "date" => param_target_date}) do
    server_id = <<"https://">> <> server_id_without_https

    with(
      {:ok, target_date} <- Front.Utils.eval_param_date(param_target_date),
      {:ok, {_target_date, zip}} <-
        Front.Utils.get_api_map_sql(server_id, target_date, true)
    ) do
      conn
      |> put_resp_content_type("application/zip")
      |> send_resp(200, zip)
    end
  end

  def show(conn, %{"server_id" => server_id_without_https}) do
    server_id = <<"https://">> <> server_id_without_https
    last_update = Front.Utils.get_target_date!(server_id)

    {:ok, {_target_date, zip}} = Front.Utils.get_api_map_sql(server_id, last_update, true)

    conn
    |> put_resp_content_type("application/zip")
    |> send_resp(200, zip)
  end
end
