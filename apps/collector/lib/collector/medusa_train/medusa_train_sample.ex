defmodule Collector.MedusaTrain.Sample do
  @enforce_keys [
    :sample,
    :labeling_dt,
    :is_inactive?
  ]

  defstruct [
    :sample,
    :labeling_dt,
    :is_inactive?
  ]

  @type t :: %__MODULE__{
          sample: Collector.MedusaPredInput.t(),
          labeling_dt: DateTime.t(),
          is_inactive?: nil | boolean()
        }
end
