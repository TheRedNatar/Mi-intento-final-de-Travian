defmodule Front.ServerControllerTest do
  use Front.ConnCase

  test "GET /api/servers", %{conn: conn} do
    conn = get(conn, ~p"/api/servers")
    response = json_response(conn, 200)

    assert(Map.has_key?(response, "data"))

    # assert json_response(conn, 200) =~ "Peace of mind from prototype to production"
  end
end
