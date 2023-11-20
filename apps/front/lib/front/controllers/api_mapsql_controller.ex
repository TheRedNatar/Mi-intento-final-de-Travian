defmodule Front.Api.MapSQLController do
  use Front, :controller

  def show(conn, %{"server_id" => server_id_without_https, "date" => param_target_date}) do
    server_id = <<"https://">> <> server_id_without_https

    with(
      {:ok, target_date} <- Front.Utils.eval_param_date(param_target_date),
      {:ok, {target_date, json}} <-
        Front.Utils.get_api_map_sql(server_id, target_date, false)
    ) do
      response_content =
        Front.Api.MapSQLJSON.show(%{json: json, target_date: target_date, zip: false})

      conn
      |> put_resp_content_type("application/json")
      |> send_resp(200, response_content)
    end
  end

  def show(conn, %{"server_id" => server_id_without_https}) do
    server_id = <<"https://">> <> server_id_without_https
    last_update = Front.Utils.get_target_date!(server_id)

    {:ok, {target_date, json}} = Front.Utils.get_api_map_sql(server_id, last_update, false)

    response_content =
      Front.Api.MapSQLJSON.show(%{json: json, target_date: target_date, zip: false})

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(200, response_content)
  end
end
