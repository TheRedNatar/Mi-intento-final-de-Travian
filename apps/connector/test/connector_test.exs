defmodule ConnectorTest do
  use ExUnit.Case
  doctest Connector

  test "greets the world" do
    assert Connector.hello() == :world
  end
end
