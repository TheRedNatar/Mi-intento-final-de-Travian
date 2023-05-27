defmodule Collector.MedusaPredOutput.Port do
  @spec open(
          model_dir :: String.t(),
          py_version :: String.t(),
          py_env :: String.t(),
          server :: String.t(),
          model :: String.t()
        ) :: {port(), reference()}
  def open(
        model_dir,
        py_version,
        py_env,
        server,
        model
      ) do
    python_path = "#{model_dir}/#{py_env}"
    medusa_server = "#{model_dir}/#{server}"
    medusa_model = "#{model_dir}/#{model}"

    env = [{'PYTHONPATH', String.to_charlist(python_path)}]

    options = [
      :binary,
      {:packet, 4},
      {:env, env}
    ]

    command = "python#{py_version} #{medusa_server} #{medusa_model}"

    port = Port.open({:spawn, command}, options)
    ref = Port.monitor(port)

    {port, ref}
  end

  @spec close(port :: port(), ref :: reference()) :: :ok
  def close(port, ref) do
    Port.demonitor(ref, [:flush])
    send(port, {self(), :close})

    receive do
      {^port, :closed} -> :ok
    after
      3_000 -> :ok
    end
  end

  @spec serialize(input :: [Collector.MedusaPredInput.t()]) :: {:ok, String.t()} | {:error, any()}
  def serialize(input), do: Jason.encode(input)

  @spec deserialize!(String.t()) :: [Collector.MedusaPredOutput.t()]
  def deserialize!(encoded_output) do
    {:ok, model_output} = Jason.decode(encoded_output)

    for [player, prediction, probability] <- model_output,
        do: %Collector.MedusaPredOutput{
          player_id: player,
          prediction: prediction,
          probability: probability
        }
  end

  @spec predict!(port :: port, medusa_pred_input :: [Collector.MedusaPredInput.t()]) :: [
          Collector.MedusaPredOutput.t()
        ]
  def predict!(port, medusa_pred_input) do
    {:ok, cmd} = serialize(medusa_pred_input)
    Port.command(port, cmd)

    receive do
      {^port, {:data, data}} -> deserialize!(data)
    end
  end
end
