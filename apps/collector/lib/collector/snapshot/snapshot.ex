defmodule Collector.Snapshot do
  @behaviour Collector.Feed

  @enforce_keys [
    :map_id,
    :x,
    :y,
    :tribe,
    :village_id,
    :village_server_id,
    :village_name,
    :player_id,
    :player_server_id,
    :player_name,
    :alliance_id,
    :alliance_server_id,
    :alliance_name,
    :population,
    :region,
    :is_capital,
    :is_city,
    :has_harbor,
    :victory_points
  ]

  defstruct [
    :map_id,
    :x,
    :y,
    :tribe,
    :village_id,
    :village_server_id,
    :village_name,
    :player_id,
    :player_server_id,
    :player_name,
    :alliance_id,
    :alliance_server_id,
    :alliance_name,
    :population,
    :region,
    :is_capital,
    :is_city,
    :has_harbor,
    :victory_points
  ]

  @type t :: %__MODULE__{
          map_id: TTypes.map_id(),
          x: TTypes.x(),
          y: TTypes.y(),
          tribe: TTypes.tribe_integer(),
          village_id: TTypes.village_id(),
          village_server_id: TTypes.village_server_id(),
          village_name: TTypes.village_name(),
          player_id: TTypes.player_id(),
          player_server_id: TTypes.player_server_id(),
          player_name: TTypes.player_name(),
          alliance_id: TTypes.alliance_id(),
          alliance_server_id: TTypes.alliance_server_id(),
          alliance_name: TTypes.alliance_name(),
          population: TTypes.population(),
          region: TTypes.region(),
          is_capital: TTypes.is_capital(),
          is_city: TTypes.is_city(),
          has_harbor: TTypes.has_harbor(),
          victory_points: TTypes.victory_points()
        }

  @impl true
  def options(), do: {"snapshot", ".c6bert"}

  @impl true
  def to_format(snapshot), do: :erlang.term_to_binary(snapshot, [:compressed, :deterministic])

  @impl true
  def from_format(encoded), do: :erlang.binary_to_term(encoded)

  def snapshot_options(), do: {"snapshot", ".c6bert"}
  def snapshot_errors_options(), do: {"snapshot_errors", ".c6bert"}

  defp snapshot_errors_to_format(snapshot_errors),
    do: :erlang.term_to_binary(snapshot_errors, [:compressed, :deterministic])

  defp snapshot_errors_from_format(encoded_snapshot_errors),
    do: :erlang.binary_to_term(encoded_snapshot_errors)

  @spec open_errors(
          root_folder :: String.t(),
          server_id :: TTypes.server_id(),
          target_date :: Date.t()
        ) ::
          {:ok, [any()]} | {:error, any()}
  def open_errors(root_folder, server_id, target_date) do
    case Storage.open(root_folder, server_id, snapshot_errors_options(), target_date) do
      {:ok, {_, encoded}} -> {:ok, snapshot_errors_from_format(encoded)}
      error -> error
    end
  end

  @spec store_errors(
          root_folder :: String.t(),
          server_id :: TTypes.server_id(),
          snapshot :: [any()],
          target_date :: Date.t()
        ) :: :ok | {:error, any()}
  def store_errors(root_folder, server_id, snapshot_errors, target_date) do
    encoded = snapshot_errors_to_format(snapshot_errors)
    Storage.store(root_folder, server_id, snapshot_errors_options(), encoded, target_date)
  end

  @impl true
  def run(root_folder, server_id, target_date, _ \\ %{}) do
    with(
      {:a, {:ok, raw_snapshot}} <-
        {:a, Collector.Feed.open(root_folder, server_id, target_date, Collector.RawSnapshot)},
      {:b, {rows, error_rows}} <- {:b, process_rows(raw_snapshot, server_id)},
      {:c, :ok} <-
        {:c, Collector.Feed.store(root_folder, server_id, target_date, rows, __MODULE__)},
      {:d, :ok} <- {:d, store_errors_if_any(root_folder, server_id, error_rows, target_date)}
    ) do
      :ok
    else
      {:a, {:error, reason}} -> {:error, {"Unable to open raw_snapshot", reason}}
      {:b, {:error, reason}} -> {:error, {"Unable to process raw_snapshot", reason}}
      {:c, {:error, reason}} -> {:error, {"Unable to store snapshot", reason}}
      {:d, {:error, reason}} -> {:error, {"Unable to store snapshot_error", reason}}
    end
  end

  @spec process_rows(raw_snapshot :: String.t(), server_id :: TTypes.server_id()) ::
          {[t()], [any()]}
  def process_rows(raw_snapshot, server_id) do
    {raw_rows, error_rows} =
      :travianmap.parse_map(raw_snapshot, :no_filter)
      |> Enum.split_with(fn {atom, _} -> atom == :ok end)

    rows = Enum.map(raw_rows, fn {:ok, row} -> create_row(server_id, row) end)
    {rows, error_rows}
  end

  defp store_errors_if_any(_, _, [], _), do: :ok

  defp store_errors_if_any(root_folder, server_id, snapshot_errors, target_date) do
    encoded = snapshot_errors_to_format(snapshot_errors)
    Storage.store(root_folder, server_id, snapshot_errors_options(), encoded, target_date)
  end

  defp create_row(
         server_id,
         {map_id, x_position, y_position, tribe, village_server_id, village_name,
          player_server_id, player_name, alliance_server_id, alliance_name, population, region,
          is_capital, is_city, has_harbor, victory_points}
       ) do
    %__MODULE__{
      map_id: map_id,
      x: x_position,
      y: y_position,
      tribe: tribe,
      village_id: make_village_id(server_id, village_server_id),
      village_server_id: village_server_id,
      village_name: village_name,
      player_id: make_player_id(server_id, player_server_id),
      player_server_id: player_server_id,
      player_name: player_name,
      alliance_id: make_alliance_id(server_id, alliance_server_id),
      alliance_server_id: alliance_server_id,
      alliance_name: alliance_name,
      population: population,
      region: region,
      is_capital: is_capital,
      is_city: is_city,
      has_harbor: has_harbor,
      victory_points: victory_points
    }
  end

  defp make_village_id(server_id, v_server_id),
    do: server_id <> "--V--" <> Integer.to_string(v_server_id)

  defp make_player_id(server_id, p_server_id),
    do: server_id <> "--P--" <> Integer.to_string(p_server_id)

  defp make_alliance_id(server_id, a_server_id),
    do: server_id <> "--A--" <> Integer.to_string(a_server_id)
end
