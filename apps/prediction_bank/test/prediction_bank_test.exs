defmodule PredictionBankTest do
  use ExUnit.Case
  doctest PredictionBank

  setup do
    mnesia_dir_test = Application.fetch_env!(:prediction_bank, :mnesia_dir_test)
    Application.put_env(:mnesia, :dir, mnesia_dir_test)
    PredictionBank.install([node()])

    on_exit(fn -> PredictionBank.uninstall([node()]) end)
  end


  test "view current servers" do
    player_id1 = "https://ttq.x2.europe.travian.com--2021-06-16--P9903"
    player_id2 = "https://ttq.x2.africa.travian.com--2021-06-16--P9903"
    state = "future_inactive"
    url1 = "https://ttq.x2.europe.travian.com"
    url2 = "https://ttq.x2.africa.travian.com"
    assert([:ok, :ok] == PredictionBank.add_players([{player_id1, state}, {player_id2, state}]))
    assert(Enum.sort([url1, url2]) == PredictionBank.current_servers())
  end

  test "select one player by server" do
    player_id1 = "https://ttq.x2.europe.travian.com--2021-06-16--P9903"
    player_id2 = "https://ttq.x2.africa.travian.com--2021-06-16--P9903"
    state = "future_inactive"
    _url1 = "https://ttq.x2.europe.travian.com"
    url2 = "https://ttq.x2.africa.travian.com"
    assert([:ok, :ok] == PredictionBank.add_players([{player_id1, state}, {player_id2, state}]))

    assert([[player_id2, state]] == PredictionBank.select(url2))
  end


  test "select multple players by server" do
    player_id1 = "https://ttq.x2.europe.travian.com--2021-06-16--P9903"
    player_id2 = "https://ttq.x2.africa.travian.com--2021-06-16--P9903"
    player_id3 = "https://ttq.x2.africa.travian.com--2021-06-16--P9904"
    state = "future_inactive"
    state2 = "inactive"
    url2 = "https://ttq.x2.africa.travian.com"
    assert([:ok, :ok, :ok] == PredictionBank.add_players([{player_id1, state}, {player_id2, state}, {player_id3, state2}]))

    assert(Enum.sort([[player_id2, state], [player_id3, state2]]) == PredictionBank.select(url2))
  end


  test "remove registers which are not updated today" do
    player_id1 = "https://ttq.x2.africa.travian.com--2021-06-16--P9902"
    player_id2 = "https://ttq.x2.africa.travian.com--2021-06-16--P9903"
    state = "future_inactive"
    url = "https://ttq.x2.africa.travian.com"
    puid2 = "P9903"
    date_yesterday = DateTime.now!("Etc/UTC") |> DateTime.to_date() |> Date.add(-1)

    func = fn -> :mnesia.write(PredictionBank.bank_players(
player_id: player_id2,
server_url: url,
player_uid: puid2,
state: state,
date: date_yesterday)) end
    :mnesia.activity(:transaction, func)

    assert([:ok] == PredictionBank.add_players([{player_id1, state}]))
    assert(Enum.sort([[player_id1, state], [player_id2, state]]) == PredictionBank.select(url))
    PredictionBank.remove_old_players()
    assert(Enum.sort([[player_id1, state]]) == PredictionBank.select(url))
  end

end
