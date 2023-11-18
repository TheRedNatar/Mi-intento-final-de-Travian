defmodule Front.MedusaView do
  use Front, :view

  # @spec distance_to_COM(x :: float(), y :: float(), row :: Collector.SMedusaPred.t()) ::
  #         String.t()
  def distance_to_COM(x, y, row) do
    int_x = String.to_integer(x)
    int_y = String.to_integer(y)

    Enum.map(row.village_coordinates, fn {row_x, row_y} ->
      distance401(int_x, int_y, row_x, row_y)
    end)
    |> Enum.min()
  end

  def yesterday_to_string(:undefined), do: "undefined"
  def yesterday_to_string(true), do: "no"
  def yesterday_to_string(false), do: "yes"

  def today_to_string(true), do: "no"
  def today_to_string(false), do: "yes"

  @spec transparent_probability(probability :: float()) :: float()
  def transparent_probability(probability) do
    case probability do
      x when x < 0.3 -> 0.3
      x -> x
    end
  end

  defp distance(width, height, x1, y1, x2, y2) do
    diff_x = abs(x1 - x2) * 1.0
    diff_y = abs(y1 - y2) * 1.0

    Float.pow(
      Float.pow(min(diff_x, width - diff_x), 2) + Float.pow(min(diff_y, height - diff_y), 2),
      0.5
    )
  end

  defp distance401(x1, y1, x2, y2), do: distance(401, 401, x1, y1, x2, y2)
end
