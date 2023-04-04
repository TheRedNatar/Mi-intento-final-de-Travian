defmodule Storage.Archive do
  @moduledoc """
  Documentation for `Archive`.
  """

  @spec candidate_for_closing?(
          root_folder :: String.t(),
          server_id :: TTypes.server_id(),
          target_date :: Date.t()
        ) :: {:ok, boolean()} | {:error, any()}
  def candidate_for_closing?(root_folder, server_id, target_date) do
    case Storage.list_dates(root_folder, server_id, {"raw_snapshot", ".c6bert"}) do
      [] ->
        {:error, "no server_id"}

      dates ->
        [last_date | _] = Enum.sort(dates, {:desc, Date})

        case Date.diff(target_date, last_date) do
          x when x >= 7 -> {:ok, true}
          _ -> {:ok, false}
        end
    end
  end

  @spec move_to_archive(root_folder :: String.t(), server_id :: TTypes.server_id()) ::
          {:ok, TTypes.server_id()} | {:error, any()}
  def move_to_archive(root_folder, server_id) do
    move_to_archive(root_folder, server_id, 0)
  end

  defp move_to_archive(root_folder, server_id, n) do
    server_id_suffix = "#{server_id}__#{n}"

    with(
      false <- Storage.exist_dir?(root_folder, {:archive, server_id_suffix}),
      source = Storage.gen_server_path(root_folder, server_id),
      dest = Storage.gen_server_path(root_folder, {:archive, server_id_suffix}),
      :ok <- File.mkdir_p(dest),
      {:ok, _} <- File.cp_r(source, dest)
    ) do
      case File.rm_rf(source) do
        {:ok, _} -> {:ok, server_id_suffix}
        {:error, posix, reason} -> {:error, {posix, reason}}
      end
    else
      true -> move_to_archive(root_folder, server_id, n + 1)
      {:error, posix} -> {:error, posix}
      {:error, posix, reason} -> {:error, {posix, reason}}
    end
  end
end
