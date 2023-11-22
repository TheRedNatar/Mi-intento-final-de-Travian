defmodule Collector.Utils do
  @moduledoc false

  @milliseconds_in_day 24 * 60 * 60 * 1000

  @spec time_until_hour(t :: Time.t()) :: non_neg_integer()
  def time_until_hour(t) do
    now = Time.utc_now()

    case Time.compare(t, now) do
      :eq -> 0
      :gt -> Time.diff(t, now, :millisecond)
      :lt -> @milliseconds_in_day + Time.diff(t, now, :millisecond)
    end
  end
end
