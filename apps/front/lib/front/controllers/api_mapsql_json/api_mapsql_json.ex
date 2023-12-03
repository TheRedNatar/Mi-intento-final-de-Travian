defmodule Front.Api.MapSQLJSON do
  def index(%{server_id: server_id, dates: dates}) do
    formatted_dates = for date <- dates, do: Date.to_iso8601(date)

    %{"server_url" => server_id, "available_dates" => formatted_dates}
  end

  def show(%{json: json, target_date: target_date, zip: false}) do
    "{
     \"zip_format\": false,
     \"target_date\": #{Jason.encode!(target_date)},
     \"data\": #{json}
   }"
  end
end
