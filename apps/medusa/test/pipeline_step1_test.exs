defmodule Medusa.Pipeline.Step1Test do
  use ExUnit.Case


  test "Analice one village" do
    input = {~D[2022-01-02], [%{
        map_id: 1,
        x: 1,
        y: 2,
        tribe: 1,
        village_id: "village_id",
        village_name: "village_name",
        player_id: "player_id",
        player_name: "player_name",
        alliance_id: "alliance_id",
        alliance_name: "alliance_name",
        population: 39
      }]}

    output = [
      %Medusa.Pipeline.Step1{
	player_id: "player_id",
	date: ~D[2022-01-02],
	total_population: 39,
	n_villages: 1,
	village_pop: %{"village_id" => 39},
	tribes_summary: %{romans: 1},
	center_mass_x: 1.0,
	center_mass_y: 2.0,
	distance_to_origin: 2.24}
    ]

    assert output == Medusa.Pipeline.Step1.process_snapshot(input)
  end


  test "Analice two village of the same player" do

    input = {~D[2022-01-02], [%{
        map_id: 1,
        x: 1,
        y: 2,
        tribe: 1,
        village_id: "village_id",
        village_name: "village_name",
        player_id: "player_id",
        player_name: "player_name",
        alliance_id: "alliance_id",
        alliance_name: "alliance_name",
        population: 39
      }, %{
        map_id: 3,
        x: 0,
        y: 2,
        tribe: 2,
        village_id: "village_id2",
        village_name: "village_name",
        player_id: "player_id",
        player_name: "player_name",
        alliance_id: "alliance_id",
        alliance_name: "alliance_name",
        population: 388
      }]}

    output = [
      %Medusa.Pipeline.Step1{
	player_id: "player_id",
	date: ~D[2022-01-02],
	total_population: 427,
	n_villages: 2,
	village_pop: %{"village_id" => 39, "village_id2" => 388},
	tribes_summary: %{romans: 1, teutons: 1},
	center_mass_x: 0.5,
	center_mass_y: 2.0,
	distance_to_origin: 2.06}
    ]

    assert output == Medusa.Pipeline.Step1.process_snapshot(input)
  end
end
