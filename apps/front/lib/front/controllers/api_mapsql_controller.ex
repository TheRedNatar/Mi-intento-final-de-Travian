defmodule Front.Api.MapSQLController do
  use Front, :controller

  action_fallback Front.ApiFallbackController

  # {
  # @api {get} /api/mapsql/:server_id Request available dates for :server_id
  # @apiName GetMapSQLDates
  # @apiGroup MapSQL
  # @apiParam {String} server_id Server unique ID.
  #
  #
  # @apiSuccess {String[]} available_dates List of available dates to use as for mapsql.
  # @apiSuccess {String} server_url Server url.
  # @apiSuccessExample {json} Success-Response:
  # HTTP/1.1 200 OK
  # {"available_dates":["2023-12-02","2023-11-26"],
  # "server_url":"https://finals.travian.com"}
  # }

  def index(conn, %{"server_id" => server_id_without_https}) do
    server_id = <<"https://">> <> server_id_without_https

    with({:ok, dates} <- Front.Utils.available_dates(server_id, :api_map_sql)) do
      render(conn, :index, server_id: server_id, dates: dates)
    end
  end

  # {
  # @api {get} /api/mapsql/:server_id/:date/:zip Request a mapsql snapshot for a :server_id
  # @apiName GetMapSQL
  # @apiGroup MapSQL
  # @apiParam {String} server_id Server unique ID.
  # @apiParam {String} date Snapshot date.
  # @apiParam {Boolean} [zip=false] Optional parameter to receive the data zipped. Please consider to use this option as true.
  #
  #
  # @apiSuccess {String[]} available_dates List of available dates to use as for mapsql.
  # @apiSuccess {String} server_url Server url.
  # @apiSuccessExample {json} Success-Response:
  # HTTP/1.1 200 OK
  # {
  #      "zip_format": false,
  #      "target_date": "2023-12-02",
  #      "data": [
  # {"y":200,"x":-190,"player_id":3239,"player_name":"Troboo","village_id":28665,"village_name":"Yeni Köy","alliance_id":27,"alliance_name":"1453","map_id":11,"region":null,"population":101,"victory_points":null,"is_capital":false,"is_city":null,"has_harbor":null,"tribe":"teutons","player_played_yesterday?":true,"player_will_play_today_prediction?":true,"prediction_confidence":0.73513},
  # {"y":200,"x":-107,"player_id":2266,"player_name":"OGUZBEY","village_id":31032,"village_name":"Yeni Köy","alliance_id":18,"alliance_name":"TR","map_id":94,"region":null,"population":19,"victory_points":null,"is_capital":false,"is_city":null,"has_harbor":null,"tribe":"huns","player_played_yesterday?":true,"player_will_play_today_prediction?":true,"prediction_confidence":0.73513},
  # 	      ...

  # 	      ]}
  # }

  def show(conn, %{
        "server_id" => server_id_without_https,
        "date" => param_target_date,
        "zip" => param_zip
      }) do
    server_id = <<"https://">> <> server_id_without_https

    with(
      {:ok, target_date} <- Front.Utils.eval_param_date(param_target_date),
      {:ok, zip} <- Front.Utils.eval_param_zip(param_zip),
      {:ok, {target_date, data}} <-
        Front.Utils.get_api_map_sql(server_id, target_date, zip)
    ) do
      case zip do
        true ->
          conn
          |> put_resp_content_type("application/zip")
          |> send_resp(200, data)

        false ->
          response_content =
            Front.Api.MapSQLJSON.show(%{json: data, target_date: target_date, zip: false})

          conn
          |> put_resp_content_type("application/json")
          |> send_resp(200, response_content)
      end
    end
  end

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
end
