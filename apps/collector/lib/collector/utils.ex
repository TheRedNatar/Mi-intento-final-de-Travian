defmodule Collector.Utils do
  @moduledoc false

  @milliseconds_in_day 24 * 60 * 60 * 1000

  @spec time_until_hour(t :: Time.t()) :: non_neg_integer()
  def time_until_hour(t) do
    now = Time.utc_now()

    case Time.compare(t, now) do
      :eq -> 0
      :gt -> Time.diff(t, now, :millisecond)
      :lt -> @milliseconds_in_day + Time.diff(t, now, :millisecond)
    end
  end

  @spec bin_to_zip_bin(content :: binary()) :: {:ok, binary()} | {:error, any()}
  def bin_to_zip_bin(content) do
    with(
      {:ok, tmp_dir} <- Temp.mkdir(),
      {:ok, {_filename, zip_bin}} <-
        :zip.zip('output.zip', [{'input.txt', content}], [
          :memory,
          {:cwd, String.to_charlist(tmp_dir)}
        ]),
      {:ok, _} <- File.rm_rf(tmp_dir)
    ) do
      {:ok, zip_bin}
    else
      error -> error
    end
  end
end
