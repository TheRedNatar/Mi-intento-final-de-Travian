defmodule Front.Utils do


  def eval_param_zip(param_zip) do
    cleaned_zip = param_zip
    |> String.trim()
    |> String.downcase()

    case cleaned_zip do
      "true" -> {:ok, true}
      "false" -> {:ok, false}
      _ -> {:error, :badarg}
    end
  end

  def eval_param_date(param_target_date), do: Date.from_iso8601(param_target_date)

  def get_target_date!(server_id) do
    pattern = {:s_server, server_id, :_, :_}
    func = fn -> :mnesia.match_object(pattern) end
    [{_, _, date, _}] = :mnesia.activity(:transaction, func)
    date
  end

  def get_api_map_sql(server_id, target_date, zip) do
    target_date_gregorian = Date.to_gregorian_days(target_date)

    pattern = {:api_map_sql, target_date_gregorian, server_id, :_}
    func = fn -> :mnesia.match_object(pattern) end

    with(
      results = :mnesia.activity(:transaction, func),
      [{_table_name, stored_target_date_gregorian, _server_id, {json, zip_json}} | _] <-
        Enum.sort_by(results, &elem(&1, 1), :desc)
    ) do
      stored_target_date = Date.from_gregorian_days(stored_target_date_gregorian)

      case zip do
        true -> {:ok, {stored_target_date, zip_json}}
        false -> {:ok, {stored_target_date, json}}
      end
    else
      x -> {:error, x}
    end
  end

  def available_dates(server_id, table_name) do

    pattern = {:api_map_sql, :_, server_id, :_}
    func = fn -> :mnesia.match_object(pattern) end
    dates = for {_, gregorian_date, _, _} <- :mnesia.activity(:sync_transaction, func), do: Date.from_gregorian_days(gregorian_date)

    {:ok, Enum.sort(dates, {:desc, Date})}
  end
end
