defmodule Collector.ServerMetadata do
  @behaviour Collector.Feed

  @enforce_keys [
    :target_dt,
    :server_id,
    :url
  ]

  defstruct [
    :target_dt,
    :server_id,
    :url,
    :name,
    :speed,
    :version,
    :number_of_tribes,
    :start_date,
    :timezone,
    :timezone_offset,
    :artifacts_date,
    :building_plans_date,
    :end_date
  ]

  @type t :: %__MODULE__{
          target_dt: DateTime.t(),
          server_id: TTypes.server_id(),
          url: String.t(),
          name: String.t() | nil,
          speed: pos_integer() | nil,
          version: String.t() | nil,
          number_of_tribes: pos_integer() | nil,
          start_date: Date.t() | nil,
          timezone: String.t() | nil,
          timezone_offset: integer() | nil,
          artifacts_date: Date.t() | nil,
          building_plans_date: Date.t() | nil,
          end_date: Date.t() | :wonder | nil
        }

  @impl true
  def options(), do: {"server_metadata", ".c6bert"}

  @impl true
  def to_format(server_metadata),
    do: :erlang.term_to_binary(server_metadata, [:compressed, :deterministic])

  @impl true
  def from_format(encoded_server_metadata),
    do: :erlang.binary_to_term(encoded_server_metadata)

  @impl true
  def run(root_folder, server_id, target_date, %{"server_metadata" => server_metadata}) do
    get = fn key -> Map.get(server_metadata, key, nil) end

    to_format_date = fn
      nil -> nil
      string_date -> Date.from_iso8601!(string_date)
    end

    end_date = get.(:end_date)

    server_metadata_struct = %__MODULE__{
      target_dt: DateTime.new!(target_date, Time.new!(0, 0, 0)),
      server_id: server_id,
      url: server_metadata[:url],
      name: get.(:name),
      speed: get.(:speed),
      version: get.(:version),
      number_of_tribes: get.(:number_of_tribes),
      start_date: to_format_date.(get.(:start_date)),
      timezone: get.(:timezone),
      timezone_offset: get.(:timezone),
      artifacts_date: to_format_date.(get.(:artifacts_date)),
      building_plans_date: to_format_date.(get.(:building_plans_date)),
      end_date: if(end_date == :wonder, do: :wonder, else: to_format_date.(end_date))
    }

    Collector.Feed.store(root_folder, server_id, target_date, server_metadata_struct, __MODULE__)
  end
end
