defmodule UtilsTest do
  use ExUnit.Case

  test "time_until_hour computes the time until we reach the hour in miliseconds, it can be on the next day" do
    now = Time.utc_now()
    one_hour = Time.add(now, 3600)
    minus_one_hour = Time.add(now, -3600)

    assert_in_delta(Collector.Utils.time_until_hour(one_hour), 3_600_000, 10)
    assert_in_delta(Collector.Utils.time_until_hour(minus_one_hour), (24 - 1) * 3_600_000, 10)
    assert_in_delta(Collector.Utils.time_until_hour(Time.utc_now() |> Time.add(1)), 1000, 10)
  end
end
