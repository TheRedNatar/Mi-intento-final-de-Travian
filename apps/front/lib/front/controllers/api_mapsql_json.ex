defmodule Front.Api.MapSQLJSON do
  @doc """
  Renders a list of server_ids.
  """
  def show(%{json: json, target_date: target_date, zip: false}) do
    "{
     \"zip_format\": false,
     \"target_date\": #{Jason.encode!(target_date)},
     \"data\": #{json}
   }"
  end
end
