defmodule TTypesTest do
  use ExUnit.Case
  doctest TTypes

  test "Ensure tribes encoding/decoding is consistant" do
    assert(1 == decode_encode(1))
    assert(2 == decode_encode(2))
    assert(3 == decode_encode(3))
    assert(4 == decode_encode(4))
    assert(5 == decode_encode(5))
    assert(6 == decode_encode(6))
    assert(7 == decode_encode(7))
    assert(8 == decode_encode(8))
  end

  defp decode_encode(tribe_int), do: TTypes.encode_tribe(TTypes.decode_tribe(tribe_int))

  test "Encode or decode an unknown tribe fails" do
    assert_raise(CaseClauseError, fn -> TTypes.encode_tribe(:unknown_tribe) end)
    assert_raise(CaseClauseError, fn -> TTypes.decode_tribe(20) end)
  end

  test "From/to server_id path" do
    server_id = "https://ts4.x1.asia.travian.com"
    assert(server_id == TTypes.server_id_from_path(TTypes.server_id_to_path(server_id)))
  end

  test "Distance perform euclidean distance" do
    assert_in_delta(TTypes.distance401(4.0, 132.0, 0.0, 0.0), 132.1, 0.1)
    assert_in_delta(TTypes.distance401(4.0, 132.0, 6.0, 134.0), 2.8, 0.1)
    assert_in_delta(TTypes.distance401(4.0, 132.0, 4.0, 128.0), 4, 0.1)
  end

  test "Distance follows toroid behaviours" do
    assert_in_delta(TTypes.distance401(4.0, 132.0, -200.0, -200.0), 208.7, 0.1)
    assert_in_delta(TTypes.distance401(4.0, 132.0, 200.0, -200.0), 207.8, 0.1)
    assert_in_delta(TTypes.distance401(4.0, 132.0, 200.0, 200.0), 207.5, 0.1)
    assert_in_delta(TTypes.distance401(4.0, 132.0, -200.0, 199.0), 208.1, 0.1)
    assert_in_delta(TTypes.distance401(4.0, 132.0, 6.0, -144.0), 125, 0.1)
  end

  test "speed_string_to_int returns the speed if follows the xNumber pattern" do
    assert(1 == TTypes.speed_string_to_int("x1"))
    assert(2 == TTypes.speed_string_to_int("x2"))
    assert(3 == TTypes.speed_string_to_int("x3"))
    assert(5 == TTypes.speed_string_to_int("x5"))
    assert(10 == TTypes.speed_string_to_int("x10"))
  end


  test "speed_int_to_string returns the pattern xNumber" do
    assert("x1" == TTypes.speed_int_to_string(1))
    assert("x2" == TTypes.speed_int_to_string(2))
    assert("x3" == TTypes.speed_int_to_string(3))
    assert("x5" == TTypes.speed_int_to_string(5))
    assert("x10" == TTypes.speed_int_to_string(10))
  end

  test "get_metadata_from_server_id returns the contraction, speed and region, otherwise error" do
    {:ok, {contraction, speed_string, region}} = TTypes.get_metadata_from_server_id("ts7.x10.france.travian.com")
    assert(contraction == "ts7")
    assert(speed_string == "x10")
    assert(region == "france")

    assert({:error, :not_splitable} == TTypes.get_metadata_from_server_id("not_splitable"))
  end
end
