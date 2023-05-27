defmodule Collector.MedusaPredOutput do
  @behaviour Collector.Feed

  @enforce_keys [
    :player_id,
    :prediction,
    :probability
  ]

  defstruct [
    :target_dt,
    :server_id,
    :player_id,
    :prediction,
    :probability
  ]

  @type t :: %__MODULE__{
          target_dt: DateTime.t() | nil,
          server_id: TTypes.server_id() | nil,
          player_id: TTypes.player_id(),
          prediction: boolean(),
          probability: float()
        }

  @impl true
  def options(), do: {"medusa_pred_output", ".c6bert"}

  @impl true
  def to_format(medusa_pred_output),
    do: :erlang.term_to_binary(medusa_pred_output, [:compressed, :deterministic])

  @impl true
  def from_format(encoded_medusa_pred_output),
    do: :erlang.binary_to_term(encoded_medusa_pred_output)

  @impl true
  def run(root_folder, server_id, target_date, %{"medusa_gen_port" => medusa_gen_port}) do
    with(
      {:a, {:ok, medusa_pred_input}} <-
        {:a, Collector.Feed.open(root_folder, server_id, target_date, Collector.MedusaPredInput)},
      {:b, {:ok, raw_output}} <-
        {:b, Collector.MedusaPredOutput.GenPort.predict(medusa_gen_port, medusa_pred_input)},
      target_dt = DateTime.new!(target_date, ~T[00:00:00.000]),
      medusa_pred_output =
        Enum.map(raw_output, fn x ->
          x
          |> Map.put(:target_dt, target_dt)
          |> Map.put(:server_id, server_id)
        end),
      {:c,
       Collector.Feed.store(root_folder, server_id, target_date, medusa_pred_output, __MODULE__)}
    ) do
      :ok
    else
      {:a, {:error, reason}} -> {:error, {"Unable to open medusa_pred_input", reason}}
      {:b, {:error, reason}} -> {:error, {"Unable to compute medusa_pred_output", reason}}
      {:c, {:error, reason}} -> {:error, {"Unable to store medusa_pred_output", reason}}
    end
  end
end
