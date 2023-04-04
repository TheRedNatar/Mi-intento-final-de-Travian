defmodule Storage do
  @moduledoc """
  Documentation for `Storage`.
  """

  @type open_options ::
          Date.t() | {Date.t(), Date.t()} | {Date.t(), Date.t(), :consecutive} | :unique

  @type dest_identifier :: :global | TTypes.server_id() | {:archive, TTypes.server_id()}
  @type date_options :: :unique | Date.t()

  @type flow_name :: binary()
  @type flow_extension :: binary()
  @type flow_options :: {flow_name(), flow_extension()}

  @spec exist?(
          root_folder :: String.t(),
          identifier :: dest_identifier(),
          flow_options :: flow_options(),
          date :: Date.t() | :unique
        ) :: boolean()
  def exist?(root_folder, identifier, {flow_name, flow_extension}, date) do
    server_path = gen_server_path(root_folder, identifier)
    {_flow_path, filename} = gen_flow_filename(server_path, date, flow_name, flow_extension)
    File.exists?(filename)
  end

  @spec exist_dir?(
          root_folder :: String.t(),
          identifier :: dest_identifier()
        ) :: boolean()
  def exist_dir?(root_folder, identifier) do
    server_path = gen_server_path(root_folder, identifier)

    case File.exists?(server_path) do
      false -> false
      true -> File.dir?(server_path)
    end
  end

  @spec store(
          root_folder :: String.t(),
          identifier :: dest_identifier(),
          flow_options :: flow_options(),
          content :: binary(),
          date :: date_options()
        ) :: :ok | {:error, any()}
  def store(
        root_folder,
        identifier,
        {flow_name, flow_extension},
        content,
        date \\ Date.utc_today()
      ) do
    server_path = gen_server_path(root_folder, identifier)
    {flow_path, filename} = gen_flow_filename(server_path, date, flow_name, flow_extension)

    case File.mkdir_p(flow_path) do
      {:error, reason} ->
        {:error, {"unable to create dir path", reason}}

      :ok ->
        case File.write(filename, content, [:binary]) do
          {:error, reason} -> {:error, {"unable to write the content", reason}}
          :ok -> :ok
        end
    end
  end

  @spec open(
          root_folder :: String.t(),
          identifier :: dest_identifier(),
          flow_options :: flow_options(),
          open_options :: open_options()
        ) :: {:ok, {Date.t(), binary()}} | {:ok, [{Date.t(), binary()}]} | {:error, any()}
  def open(root_folder, identifier, flow_options, {start_date, end_date}) do
    case Date.compare(start_date, end_date) do
      :gt ->
        {:error, "end_date earlier than start_date"}

      :eq ->
        {:error, "end_date and start_date can't be the same"}

      :lt ->
        result =
          gen_date_range!(start_date, end_date)
          |> Enum.map(&open(root_folder, identifier, flow_options, &1))
          |> Enum.filter(fn {atom, _} -> atom == :ok end)
          |> Enum.map(fn {_, content} -> content end)

        {:ok, result}
    end
  end

  def open(root_folder, identifier, flow_options, {start_date, end_date, :consecutive}) do
    case Date.compare(start_date, end_date) do
      :gt ->
        {:error, "end_date earlier than start_date"}

      :eq ->
        open(root_folder, identifier, flow_options, start_date)

      :lt ->
        result =
          gen_date_range!(start_date, end_date)
          |> open_rec([], root_folder, identifier, flow_options)

        {:ok, result}
    end
  end

  def open(root_folder, identifier, {flow_name, flow_extension}, date) do
    server_path = gen_server_path(root_folder, identifier)
    {_flow_path, filename} = gen_flow_filename(server_path, date, flow_name, flow_extension)

    case File.read(filename) do
      {:ok, content} -> {:ok, {date, content}}
      {:error, reason} -> {:error, {"unable to open the file", reason}}
    end
  end

  defp open_rec([], contents, _root_folder, _identifier, _flow_options), do: contents

  defp open_rec([date | dates], contents, root_folder, identifier, flow_options) do
    case open(root_folder, identifier, flow_options, date) do
      {:error, _reason} ->
        contents

      {:ok, content} ->
        open_rec(dates, contents ++ [content], root_folder, identifier, flow_options)
    end
  end

  @spec gen_server_path(
          root_folder :: binary(),
          identifier :: dest_identifier()
        ) :: binary()
  def gen_server_path(root_folder, :global), do: "#{root_folder}/global"

  def gen_server_path(root_folder, {:archive, server_id}),
    do: "#{root_folder}/archive/#{TTypes.server_id_to_path(server_id)}"

  def gen_server_path(root_folder, server_id),
    do: "#{gen_servers_path(root_folder)}/#{TTypes.server_id_to_path(server_id)}"

  defp gen_servers_path(root_folder), do: "#{root_folder}/servers"

  @spec gen_flow_filename(
          dir_path :: binary(),
          date :: date_options(),
          flow_name :: flow_name(),
          flow_extension :: flow_extension()
        ) :: {String.t(), String.t()}
  defp gen_flow_filename(dir_path, :unique, flow_name, flow_extension) do
    flow_path = "#{dir_path}/unique"
    filename = "#{flow_path}/#{flow_name}#{flow_extension}"
    {flow_path, filename}
  end

  defp gen_flow_filename(dir_path, date, flow_name, flow_extension) do
    flow_path = "#{dir_path}/#{flow_name}"
    filename = "#{flow_path}/date_#{Date.to_iso8601(date, :basic)}#{flow_extension}"
    {flow_path, filename}
  end

  @spec gen_date_range!(start_date :: Date.t(), end_date :: Date.t()) :: [Date.t()]
  def gen_date_range!(start_date, end_date) do
    diff = Date.diff(end_date, start_date)
    for i <- diff..0, do: Date.add(start_date, i)
  end

  @spec list_dates(
          root_folder :: String.t(),
          identifier :: dest_identifier(),
          flow_options :: flow_options
        ) :: [Date.t()]
  def list_dates(root_folder, identifier, {flow_name, _}) do
    dir_path = gen_server_path(root_folder, identifier)
    flow_path = "#{dir_path}/#{flow_name}"

    case File.exists?(dir_path) do
      false -> []
      true -> File.ls!(flow_path) |> Enum.map(&filename_to_date/1)
    end
  end

  @spec list_servers(root_folder :: String.t()) :: [TTypes.server_id()]
  def list_servers(root_folder) do
    servers_path = gen_servers_path(root_folder)

    with(
      true <- File.exists?(servers_path),
      true <- File.dir?(servers_path)
    ) do
      for server_id_path <- File.ls!(servers_path), do: TTypes.server_id_from_path(server_id_path)
    else
      _ -> []
    end
  end

  defp filename_to_date(
         <<"date_", s_year::binary-size(4), s_month::binary-size(2), s_day::binary-size(2),
           _::binary>>
       ) do
    year = String.to_integer(s_year)
    month = String.to_integer(s_month)
    day = String.to_integer(s_day)
    Date.new!(year, month, day)
  end
end
