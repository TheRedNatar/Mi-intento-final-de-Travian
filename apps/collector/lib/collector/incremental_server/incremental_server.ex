defmodule Collector.IncrementalServer do
  require Logger

  @enforce_keys [
    :target_date,
    :extraction_date,
    :server_id,
    :server_url,
    :server_contraction,
    :server_speed,
    :server_region,
    :estimated_starting_date,
    :incremental
  ]

  defstruct [
    :target_date,
    :extraction_date,
    :server_id,
    :server_url,
    :server_contraction,
    :server_speed,
    :server_region,
    :estimated_starting_date,
    :incremental
  ]

  @type t :: %__MODULE__{
          target_date: DateTime.t(),
          extraction_date: DateTime.t(),
          server_id: TTypes.server_id(),
	  server_url: String.t(),
          server_contraction: nil | String.t(),
          server_speed: nil | pos_integer(),
          server_region: nil | String.t(),
          estimated_starting_date: Date.t(),
	  incremental: [Collector.IncrementalServer.Entities.t()]
        }
  end
