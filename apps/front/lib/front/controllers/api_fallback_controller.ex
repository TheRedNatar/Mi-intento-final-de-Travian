defmodule Front.ApiFallbackController do
  use Phoenix.Controller

  def call(conn, {:error, _}) do
    conn
    |> put_status(:not_found)
    |> put_view(json: Front.ErrorJSON)
    |> render(:"404")
  end
end
