defmodule Medusa.GenPort.Test do
  use ExUnit.Case

  test "Exit if the Python model goes down" do
    state = %Medusa.GenPort{
      model_dir: System.get_env("MITRAVIAN__MEDUSA_MODEL_DIR", "priv"),
      setup: false,
      port: self(),
      ref: :erlang.make_ref()
    }
    reason = "Some reason"
    msg_down = {:DOWN, state.ref, :port, state.port, reason}
    output = {:stop, {"Python model down", reason}, state}

    assert(Medusa.GenPort.handle_info(msg_down, state) == output)
  end

  test "Predict only if the setup is done" do
    state = %Medusa.GenPort{
      model_dir: System.get_env("MITRAVIAN__MEDUSA_MODEL_DIR", "priv"),
      setup: false,
      port: self(),
      ref: :erlang.make_ref()
    }
    msg = {:predict, ["bad_data"]}
    output = {:noreply, state}

    assert(Medusa.GenPort.handle_call(msg, self(), state) == output)
  end

  test "Predict in normal behaviour" do
    model_dir = System.get_env("MITRAVIAN__MEDUSA_MODEL_DIR", "priv")
    {:ok, pid} = Medusa.GenPort.start_link(model_dir)


    fen = %Medusa.Pipeline.FEN{
    player_id: "player_id_n",
    date: ~D[2022-01-02],
    inactive_in_current: false,
    n_days: 3,
    dow: 7,
    total_population: 100,
    total_population_increase: 50,
    total_population_decrease: 10,
    n_villages: 3,
    n_village_increase: 0,
    n_village_decrease: 0,
    tribes_summary: %{romans: 3},
    center_mass_x: 1,
    center_mass_y: 1,
    distance_to_origin: 2,
    prev_distance_to_origin: 2
    }


    fe1 = %Medusa.Pipeline.FE1{

    player_id: "player_id_1",
    date: ~D[2022-01-02],
    inactive_in_current: :undefined,
    total_population: 100,
    n_villages: 3,
    tribes_summary: %{romans: 3},
    center_mass_x: 1,
    center_mass_y: 1,
    distance_to_origin: 2,
    }

    sample_n = %Medusa.Pipeline.Step2{fe_type: :ndays_n, fe_struct: fen}
    sample_1 = %Medusa.Pipeline.Step2{fe_type: :ndays_1, fe_struct: fe1}

    samples = [sample_n, sample_1]

    {:ok, predictions} = Medusa.GenPort.predict(pid, samples)
    for pred <- predictions, do: assert_pred(pred)
  end


  defp assert_pred(pred) do
    assert(is_boolean(pred.inactive_in_future))
    assert(pred.model == :player_n or pred.model == :player_1)
  end



end
