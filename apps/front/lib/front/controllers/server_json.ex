defmodule Front.Api.ServerJSON do
  @doc """
  Renders a list of server_ids.
  """
  def index(%{servers: servers}) do
    %{
      data: for(server <- servers, do: process(server))
    }
  end

  defp process(<<"https://", server::binary>>), do: server
end
