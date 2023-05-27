defmodule Collector.MedusaPredOutput.GenPort do
  use GenServer
  require Logger

  defstruct [
    :model_dir,
    :py_version,
    :py_env,
    :server,
    :model,
    :port,
    :ref
  ]

  @spec predict(gen_port :: GenServer.server(), data :: [Collector.MedusaPredInput.t()]) ::
          {:ok, [Collector.MedusaPredOutput.t()]} | {:error, any()}
  def predict(gen_port, data) do
    try do
      GenServer.call(gen_port, {:predict, data}, 15_000)
    rescue
      e in RuntimeError -> {:error, e}
    end
  end

  @spec start_link(
          model_dir :: String.t(),
          py_version :: String.t(),
          py_env :: String.t(),
          server :: String.t(),
          model :: String.t()
        ) :: GenServer.on_start()
  def start_link(model_dir, py_version, py_env, server, model),
    do:
      GenServer.start_link(__MODULE__, [model_dir, py_version, py_env, server, model],
        name: __MODULE__
      )

  @impl true
  def init([model_dir, py_version, py_env, server, model]) do
    state = %__MODULE__{
      model_dir: model_dir,
      py_version: py_version,
      py_env: py_env,
      server: server,
      model: model
    }

    {:ok, state, {:continue, :start_port}}
  end

  @impl true
  def handle_continue(:start_port, state) do
    {port, ref} =
      Collector.MedusaPredOutput.Port.open(
        state.model_dir,
        state.py_version,
        state.py_env,
        state.server,
        state.model
      )

    Logger.debug(%{msg: "GenPort model loaded", args: state})

    new_state =
      state
      |> Map.put(:port, port)
      |> Map.put(:ref, ref)

    {:noreply, new_state}
  end

  @impl true
  def handle_call({:predict, data}, _, state) do
    predictions = Collector.MedusaPredOutput.Port.predict!(state.port, data)
    {:reply, {:ok, predictions}, state}
  end

  def handle_call(_, _, state), do: {:noreply, state}

  @impl true
  def handle_cast(_, state), do: {:noreply, state}

  @impl true
  def handle_info({:DOWN, ref, :port, port, reason}, state = %{ref: ref, port: port}) do
    Logger.error(%{msg: "Python model down", reason: reason, state: state})

    new_state =
      state
      |> Map.put(:port, nil)
      |> Map.put(:ref, nil)

    {:noreply, new_state, {:continue, :start_port}}
  end

  def handle_info(_, state), do: {:noreply, state}

  @impl true
  def terminate(_, state) do
    Collector.MedusaPredOutput.Port.close(state.port, state.ref)
  end
end
