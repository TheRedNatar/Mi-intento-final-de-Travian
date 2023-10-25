defmodule Collector.MedusaPredOutputTest do
  use ExUnit.Case

  @tag :tmp_dir
  test "run fails if there is no medusa_pred_input", %{tmp_dir: root_folder} do
    target_date = Date.utc_today()
    server_id = "server1"

    {:error, {msg, _reason}} =
      Collector.MedusaPredOutput.run(root_folder, server_id, target_date, %{
        "medusa_gen_port" => "medusa_gen_port"
      })

    assert(msg == "Unable to open medusa_pred_input")
  end

  @tag :tmp_dir
  test "run fails if there is no medusa_gen_port", %{tmp_dir: root_folder} do
    target_date = Date.utc_today()
    server_id = "server1"

    assert_raise(FunctionClauseError, fn ->
      Collector.MedusaPredOutput.run(root_folder, server_id, target_date, %{})
    end)
  end

  @tag tmp_dir: true, needs_model: true
  test "run creates a prediction output feed", %{tmp_dir: root_folder} do
    server_id = "https://gos.x2.arabics.travian.com"
    target_dt = ~U[2023-05-28 00:00:00.000Z]
    player_id = "https://gos.x2.arabics.travian.com--P--1004"
    target_date = DateTime.to_date(target_dt)

    medusa_pred_input = [
      %Collector.MedusaPredInput{
        target_dt: ~U[2023-05-28 00:00:00.000Z],
        server_id: "https://gos.x2.arabics.travian.com",
        player_id: "https://gos.x2.arabics.travian.com--P--1004",
        has_alliance?: 1,
        server_days_from_start: 0,
        has_speed?: 1,
        speed: 2,
        player_days_from_start: 0,
        estimated_tribe: 3,
        t_has_increase?: 0,
        t_total_population: 3417,
        t_population_increase: 0,
        t_population_increase_by_founded: 0,
        t_population_increase_by_conquered: 0,
        t_population_decrease: 0,
        t_population_decrease_by_conquered: 0,
        t_population_decrease_by_destroyed: 0,
        t_total_villages: 8,
        t_n_villages_with_population_increase: 0,
        t_n_villages_with_population_decrease: 0,
        t_n_villages_with_population_stuck: 0,
        t_new_village_founded: 0,
        t_new_village_conquered: 0,
        t_lost_village_conquered: 0,
        t_lost_village_destroyed: 0,
        t_1_has_data?: 0,
        t_1_has_increase?: 0,
        t_1_time_difference_in_days: 0,
        t_1_total_population: 0,
        t_1_population_increase: 0,
        t_1_population_increase_by_founded: 0,
        t_1_population_increase_by_conquered: 0,
        t_1_population_decrease: 0,
        t_1_population_decrease_by_conquered: 0,
        t_1_population_decrease_by_destroyed: 0,
        t_1_total_villages: 0,
        t_1_n_villages_with_population_increase: 0,
        t_1_n_villages_with_population_decrease: 0,
        t_1_n_villages_with_population_stuck: 0,
        t_1_new_village_founded: 0,
        t_1_new_village_conquered: 0,
        t_1_lost_village_conquered: 0,
        t_1_lost_village_destroyed: 0,
        t_2_has_data?: 0,
        t_2_has_increase?: 0,
        t_2_time_difference_in_days: 0,
        t_2_total_population: 0,
        t_2_population_increase: 0,
        t_2_population_increase_by_founded: 0,
        t_2_population_increase_by_conquered: 0,
        t_2_population_decrease: 0,
        t_2_population_decrease_by_conquered: 0,
        t_2_population_decrease_by_destroyed: 0,
        t_2_total_villages: 0,
        t_2_n_villages_with_population_increase: 0,
        t_2_n_villages_with_population_decrease: 0,
        t_2_n_villages_with_population_stuck: 0,
        t_2_new_village_founded: 0,
        t_2_new_village_conquered: 0,
        t_2_lost_village_conquered: 0,
        t_2_lost_village_destroyed: 0,
        t_3_has_data?: 0,
        t_3_has_increase?: 0,
        t_3_time_difference_in_days: 0,
        t_3_total_population: 0,
        t_3_population_increase: 0,
        t_3_population_increase_by_founded: 0,
        t_3_population_increase_by_conquered: 0,
        t_3_population_decrease: 0,
        t_3_population_decrease_by_conquered: 0,
        t_3_population_decrease_by_destroyed: 0,
        t_3_total_villages: 0,
        t_3_n_villages_with_population_increase: 0,
        t_3_n_villages_with_population_decrease: 0,
        t_3_n_villages_with_population_stuck: 0,
        t_3_new_village_founded: 0,
        t_3_new_village_conquered: 0,
        t_3_lost_village_conquered: 0,
        t_3_lost_village_destroyed: 0,
        t_4_has_data?: 0,
        t_4_has_increase?: 0,
        t_4_time_difference_in_days: 0,
        t_4_total_population: 0,
        t_4_population_increase: 0,
        t_4_population_increase_by_founded: 0,
        t_4_population_increase_by_conquered: 0,
        t_4_population_decrease: 0,
        t_4_population_decrease_by_conquered: 0,
        t_4_population_decrease_by_destroyed: 0,
        t_4_total_villages: 0,
        t_4_n_villages_with_population_increase: 0,
        t_4_n_villages_with_population_decrease: 0,
        t_4_n_villages_with_population_stuck: 0,
        t_4_new_village_founded: 0,
        t_4_new_village_conquered: 0,
        t_4_lost_village_conquered: 0,
        t_4_lost_village_destroyed: 0,
        t_5_has_data?: 0,
        t_5_has_increase?: 0,
        t_5_time_difference_in_days: 0,
        t_5_total_population: 0,
        t_5_population_increase: 0,
        t_5_population_increase_by_founded: 0,
        t_5_population_increase_by_conquered: 0,
        t_5_population_decrease: 0,
        t_5_population_decrease_by_conquered: 0,
        t_5_population_decrease_by_destroyed: 0,
        t_5_total_villages: 0,
        t_5_n_villages_with_population_increase: 0,
        t_5_n_villages_with_population_decrease: 0,
        t_5_n_villages_with_population_stuck: 0,
        t_5_new_village_founded: 0,
        t_5_new_village_conquered: 0,
        t_5_lost_village_conquered: 0,
        t_5_lost_village_destroyed: 0,
        t_6_has_data?: 0,
        t_6_has_increase?: 0,
        t_6_time_difference_in_days: 0,
        t_6_total_population: 0,
        t_6_population_increase: 0,
        t_6_population_increase_by_founded: 0,
        t_6_population_increase_by_conquered: 0,
        t_6_population_decrease: 0,
        t_6_population_decrease_by_conquered: 0,
        t_6_population_decrease_by_destroyed: 0,
        t_6_total_villages: 0,
        t_6_n_villages_with_population_increase: 0,
        t_6_n_villages_with_population_decrease: 0,
        t_6_n_villages_with_population_stuck: 0,
        t_6_new_village_founded: 0,
        t_6_new_village_conquered: 0,
        t_6_lost_village_conquered: 0,
        t_6_lost_village_destroyed: 0,
        t_7_has_data?: 0,
        t_7_has_increase?: 0,
        t_7_time_difference_in_days: 0,
        t_7_total_population: 0,
        t_7_population_increase: 0,
        t_7_population_increase_by_founded: 0,
        t_7_population_increase_by_conquered: 0,
        t_7_population_decrease: 0,
        t_7_population_decrease_by_conquered: 0,
        t_7_population_decrease_by_destroyed: 0,
        t_7_total_villages: 0,
        t_7_n_villages_with_population_increase: 0,
        t_7_n_villages_with_population_decrease: 0,
        t_7_n_villages_with_population_stuck: 0,
        t_7_new_village_founded: 0,
        t_7_new_village_conquered: 0,
        t_7_lost_village_conquered: 0,
        t_7_lost_village_destroyed: 0
      }
    ]

    :ok =
      Collector.Feed.store(
        root_folder,
        server_id,
        target_date,
        medusa_pred_input,
        Collector.MedusaPredInput
      )

    :ok = Application.ensure_started(:collector)

    assert(
      :ok ==
        Collector.MedusaPredOutput.run(root_folder, server_id, target_date, %{
          "medusa_gen_port" => Collector.MedusaPredOutput.GenPort
        })
    )

    {:ok, medusa_pred_output} =
      Collector.Feed.open(root_folder, server_id, target_date, Collector.MedusaPredOutput)

    assert(length(medusa_pred_output) == 1)

    [pred] = medusa_pred_output
    assert(pred.server_id == server_id)
    assert(pred.target_dt == target_dt)
    assert(pred.player_id == player_id)
    assert(is_boolean(pred.prediction))
    assert(is_float(pred.probability))
    assert(pred.probability <= 1)
    assert(pred.probability >= 0)
  end
end
