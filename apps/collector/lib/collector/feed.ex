defmodule Collector.Feed do


  @enforce_keys [
    :server_id,
    :target_dt,
    :target_date,
    :feed,
    :started_dt,
    :duration_in_seconds
  ]

  @derive Jason.Encoder
  defstruct [
    :server_id,
    :target_dt,
    :target_date,
    :feed,
    :started_dt,
    :duration_in_seconds
  ]


  @type t :: %__MODULE__{
          server_id: TTypes.server_id(),
	  target_dt: DateTime.t(),
	  target_date: Date.t(),
	  feed: String.t(),
	  started_dt: DateTime.t(),
	  duration_in_seconds: float()
        }


  @doc"""
  Name and encoding of the feed
  """
  @callback options() :: {String.t(), String.t()}


  @doc"""
  Serialize the feed
  """
  @callback to_format(feed_struct :: any()) :: binary()
  

  @doc"""
  Deserialize the feed
  """
  @callback from_format(binary()) :: any()


  @doc"""
  Transforms a feed in to another feed, and then stores it
  """
  @callback run(root_folder :: String.t(), server_id :: TTypes.server_id(), target_date :: Date.t()) :: any()


  @spec run_feed(root_folder :: String.t(), server_id :: TTypes.server_id(), target_date :: Date.t(), feed_module :: module) :: :ok | {:error, any()}
  def run_feed(root_folder, server_id, target_date, feed) do
    {feed_name, _} = feed.options()
    started_dt = DateTime.utc_now()
    with(
      :ok <- feed.run(root_folder, server_id, target_date)
    ) do
      end_dt = DateTime.utc_now()
      duration_in_seconds = DateTime.diff(end_dt, started_dt, :millisecond) / 1000
      metadata = %__MODULE__{
        server_id: server_id,
	target_dt: DateTime.new!(target_date, ~T[00:00:00.000]),
	target_date: target_date,
	feed: feed_name,
	started_dt: started_dt,
	duration_in_seconds: duration_in_seconds
      }
      store_metadata(root_folder, server_id, target_date, metadata, feed)
    end
  end

  @spec open(root_folder :: String.t(), server_id :: TTypes.server_id(), target_date :: Date.t(), feed_module :: module()) :: {:ok, any()} | {:error, any()}
  def open(root_folder, server_id, target_date, feed) do
    case Storage.open(root_folder, server_id, feed.options(), target_date) do
      {:ok, {_, encoded}} -> {:ok, feed.from_format(encoded)}
      error -> error
    end
  end


  @spec store(root_folder :: String.t(), server_id :: TTypes.server_id(), target_date :: Date.t(), content :: any(), feed_module :: module()) :: :ok | {:error, any()}
  def store(root_folder, server_id, target_date, content, feed) do
    encoded = feed.to_format(content)
    Storage.store(root_folder, server_id, feed.options(), encoded, target_date)
  end


  defp store_metadata(root_folder, server_id, target_date, metadata, feed) do
    {feed_name, _} = feed.options()
    encoded = Jason.encode!(metadata) |> Jason.Formatter.pretty_print()
    name = "metadata_#{feed_name}"
    Storage.store(root_folder, server_id, {name, ".json"}, encoded, target_date)
  end




end
