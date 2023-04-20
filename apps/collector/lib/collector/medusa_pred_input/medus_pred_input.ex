defmodule Collector.MedusaPredInput do
  @behaviour Collector.Feed

  @enforce_keys [
    :target_dt,
    :server_id,
    # Snapshot
    :player_id,
    :has_alliance?,
    # AggServer
    :server_days_from_start,
    :has_speed?,
    :speed,
    # AggPlayers
    :player_days_from_start,
    :estimated_tribe,
    ## Today's data
    :t_has_increase?,
    :t_total_population,
    :t_population_increase,
    :t_population_increase_by_founded,
    :t_population_increase_by_conquered,
    :t_population_decrease,
    :t_population_decrease_by_conquered,
    :t_population_decrease_by_destroyed,
    :t_total_villages,
    :t_n_villages_with_population_increase,
    :t_n_villages_with_population_decrease,
    :t_n_villages_with_population_stuck,
    :t_new_village_founded,
    :t_new_village_conquered,
    :t_lost_village_conquered,
    :t_lost_village_destroyed
  ]

  defstruct [
    :target_dt,
    :server_id,
    # Snapshot
    :player_id,
    :has_alliance?,
    # AggServer
    :server_days_from_start,
    :has_speed?,
    :speed,
    # AggPlayers
    :player_days_from_start,
    :estimated_tribe,
    ## Today's data
    :t_has_increase?,
    :t_total_population,
    :t_population_increase,
    :t_population_increase_by_founded,
    :t_population_increase_by_conquered,
    :t_population_decrease,
    :t_population_decrease_by_conquered,
    :t_population_decrease_by_destroyed,
    :t_total_villages,
    :t_n_villages_with_population_increase,
    :t_n_villages_with_population_decrease,
    :t_n_villages_with_population_stuck,
    :t_new_village_founded,
    :t_new_village_conquered,
    :t_lost_village_conquered,
    :t_lost_village_destroyed,
    ## Today - 1 sample
    # Represents a boolean value as false
    t_1_has_data?: 0,
    # Represents a boolean value as false
    t_1_has_increase?: 0,
    t_1_time_difference_in_days: 0,
    t_1_total_population: 0,
    t_1_population_increase: 0,
    t_1_population_increase_by_founded: 0,
    t_1_population_increase_by_conquered: 0,
    t_1_population_decrease: 0,
    t_1_population_decrease_by_conquered: 0,
    t_1_population_decrease_by_destroyed: 0,
    t_1_total_villages: 0,
    t_1_n_villages_with_population_increase: 0,
    t_1_n_villages_with_population_decrease: 0,
    t_1_n_villages_with_population_stuck: 0,
    t_1_new_village_founded: 0,
    t_1_new_village_conquered: 0,
    t_1_lost_village_conquered: 0,
    t_1_lost_village_destroyed: 0,
    ## Today - 2 sample
    # Represents a boolean value as false
    t_2_has_data?: 0,
    # Represents a boolean value as false
    t_2_has_increase?: 0,
    t_2_time_difference_in_days: 0,
    t_2_total_population: 0,
    t_2_population_increase: 0,
    t_2_population_increase_by_founded: 0,
    t_2_population_increase_by_conquered: 0,
    t_2_population_decrease: 0,
    t_2_population_decrease_by_conquered: 0,
    t_2_population_decrease_by_destroyed: 0,
    t_2_total_villages: 0,
    t_2_n_villages_with_population_increase: 0,
    t_2_n_villages_with_population_decrease: 0,
    t_2_n_villages_with_population_stuck: 0,
    t_2_new_village_founded: 0,
    t_2_new_village_conquered: 0,
    t_2_lost_village_conquered: 0,
    t_2_lost_village_destroyed: 0,
    ## Today - 3 sample
    # Represents a boolean value as false
    t_3_has_data?: 0,
    # Represents a boolean value as false
    t_3_has_increase?: 0,
    t_3_time_difference_in_days: 0,
    t_3_total_population: 0,
    t_3_population_increase: 0,
    t_3_population_increase_by_founded: 0,
    t_3_population_increase_by_conquered: 0,
    t_3_population_decrease: 0,
    t_3_population_decrease_by_conquered: 0,
    t_3_population_decrease_by_destroyed: 0,
    t_3_total_villages: 0,
    t_3_n_villages_with_population_increase: 0,
    t_3_n_villages_with_population_decrease: 0,
    t_3_n_villages_with_population_stuck: 0,
    t_3_new_village_founded: 0,
    t_3_new_village_conquered: 0,
    t_3_lost_village_conquered: 0,
    t_3_lost_village_destroyed: 0,
    ## Today - 4 sample
    # Represents a boolean value as false
    t_4_has_data?: 0,
    # Represents a boolean value as false
    t_4_has_increase?: 0,
    t_4_time_difference_in_days: 0,
    t_4_total_population: 0,
    t_4_population_increase: 0,
    t_4_population_increase_by_founded: 0,
    t_4_population_increase_by_conquered: 0,
    t_4_population_decrease: 0,
    t_4_population_decrease_by_conquered: 0,
    t_4_population_decrease_by_destroyed: 0,
    t_4_total_villages: 0,
    t_4_n_villages_with_population_increase: 0,
    t_4_n_villages_with_population_decrease: 0,
    t_4_n_villages_with_population_stuck: 0,
    t_4_new_village_founded: 0,
    t_4_new_village_conquered: 0,
    t_4_lost_village_conquered: 0,
    t_4_lost_village_destroyed: 0,
    ## Today - 5 sample
    # Represents a boolean value as false
    t_5_has_data?: 0,
    # Represents a boolean value as false
    t_5_has_increase?: 0,
    t_5_time_difference_in_days: 0,
    t_5_total_population: 0,
    t_5_population_increase: 0,
    t_5_population_increase_by_founded: 0,
    t_5_population_increase_by_conquered: 0,
    t_5_population_decrease: 0,
    t_5_population_decrease_by_conquered: 0,
    t_5_population_decrease_by_destroyed: 0,
    t_5_total_villages: 0,
    t_5_n_villages_with_population_increase: 0,
    t_5_n_villages_with_population_decrease: 0,
    t_5_n_villages_with_population_stuck: 0,
    t_5_new_village_founded: 0,
    t_5_new_village_conquered: 0,
    t_5_lost_village_conquered: 0,
    t_5_lost_village_destroyed: 0,
    ## Today - 6 sample
    # Represents a boolean value as false
    t_6_has_data?: 0,
    # Represents a boolean value as false
    t_6_has_increase?: 0,
    t_6_time_difference_in_days: 0,
    t_6_total_population: 0,
    t_6_population_increase: 0,
    t_6_population_increase_by_founded: 0,
    t_6_population_increase_by_conquered: 0,
    t_6_population_decrease: 0,
    t_6_population_decrease_by_conquered: 0,
    t_6_population_decrease_by_destroyed: 0,
    t_6_total_villages: 0,
    t_6_n_villages_with_population_increase: 0,
    t_6_n_villages_with_population_decrease: 0,
    t_6_n_villages_with_population_stuck: 0,
    t_6_new_village_founded: 0,
    t_6_new_village_conquered: 0,
    t_6_lost_village_conquered: 0,
    t_6_lost_village_destroyed: 0,
    ## Today - 7 sample
    # Represents a boolean value as false
    t_7_has_data?: 0,
    # Represents a boolean value as false
    t_7_has_increase?: 0,
    t_7_time_difference_in_days: 0,
    t_7_total_population: 0,
    t_7_population_increase: 0,
    t_7_population_increase_by_founded: 0,
    t_7_population_increase_by_conquered: 0,
    t_7_population_decrease: 0,
    t_7_population_decrease_by_conquered: 0,
    t_7_population_decrease_by_destroyed: 0,
    t_7_total_villages: 0,
    t_7_n_villages_with_population_increase: 0,
    t_7_n_villages_with_population_decrease: 0,
    t_7_n_villages_with_population_stuck: 0,
    t_7_new_village_founded: 0,
    t_7_new_village_conquered: 0,
    t_7_lost_village_conquered: 0,
    t_7_lost_village_destroyed: 0
  ]

  @type t :: %__MODULE__{
          target_dt: DateTime.t(),
          server_id: TTypes.server_id(),
          player_id: TTypes.player_id(),
          has_alliance?: non_neg_integer(),
          # AggServer
          server_days_from_start: non_neg_integer(),
          has_speed?: non_neg_integer(),
          speed: non_neg_integer(),
          # AggPlayers
          player_days_from_start: non_neg_integer(),
          estimated_tribe: TTypes.tribe_integer(),
          ## Today's data
          t_has_increase?: non_neg_integer(),
          t_total_population: non_neg_integer(),
          t_population_increase: non_neg_integer(),
          t_population_increase_by_founded: non_neg_integer(),
          t_population_increase_by_conquered: non_neg_integer(),
          t_population_decrease: non_neg_integer(),
          t_population_decrease_by_conquered: non_neg_integer(),
          t_population_decrease_by_destroyed: non_neg_integer(),
          t_total_villages: pos_integer(),
          t_n_villages_with_population_increase: non_neg_integer(),
          t_n_villages_with_population_decrease: non_neg_integer(),
          t_n_villages_with_population_stuck: non_neg_integer(),
          t_new_village_founded: non_neg_integer(),
          t_new_village_conquered: non_neg_integer(),
          t_lost_village_conquered: non_neg_integer(),
          t_lost_village_destroyed: non_neg_integer(),
          ## Today - 1 sample
          # Represents a boolean value as 0 or 1
          t_1_has_data?: non_neg_integer(),
          # Represents a boolean value as 0 or 1
          t_1_has_increase?: non_neg_integer(),
          t_1_time_difference_in_days: number(),
          t_1_total_population: non_neg_integer(),
          t_1_population_increase: non_neg_integer(),
          t_1_population_increase_by_founded: non_neg_integer(),
          t_1_population_increase_by_conquered: non_neg_integer(),
          t_1_population_decrease: non_neg_integer(),
          t_1_population_decrease_by_conquered: non_neg_integer(),
          t_1_population_decrease_by_destroyed: non_neg_integer(),
          t_1_total_villages: non_neg_integer(),
          t_1_n_villages_with_population_increase: non_neg_integer(),
          t_1_n_villages_with_population_decrease: non_neg_integer(),
          t_1_n_villages_with_population_stuck: non_neg_integer(),
          t_1_new_village_founded: non_neg_integer(),
          t_1_new_village_conquered: non_neg_integer(),
          t_1_lost_village_conquered: non_neg_integer(),
          t_1_lost_village_destroyed: non_neg_integer(),
          ## Today - 2 sample
          # Represents a boolean value as 0 or 1
          t_2_has_data?: non_neg_integer(),
          # Represents a boolean value as 0 or 1
          t_2_has_increase?: non_neg_integer(),
          t_2_time_difference_in_days: number(),
          t_2_total_population: non_neg_integer(),
          t_2_population_increase: non_neg_integer(),
          t_2_population_increase_by_founded: non_neg_integer(),
          t_2_population_increase_by_conquered: non_neg_integer(),
          t_2_population_decrease: non_neg_integer(),
          t_2_population_decrease_by_conquered: non_neg_integer(),
          t_2_population_decrease_by_destroyed: non_neg_integer(),
          t_2_total_villages: non_neg_integer(),
          t_2_n_villages_with_population_increase: non_neg_integer(),
          t_2_n_villages_with_population_decrease: non_neg_integer(),
          t_2_n_villages_with_population_stuck: non_neg_integer(),
          t_2_new_village_founded: non_neg_integer(),
          t_2_new_village_conquered: non_neg_integer(),
          t_2_lost_village_conquered: non_neg_integer(),
          t_2_lost_village_destroyed: non_neg_integer(),
          ## Today - 3 sample
          # Represents a boolean value as 0 or 1
          t_3_has_data?: non_neg_integer(),
          # Represents a boolean value as 0 or 1
          t_3_has_increase?: non_neg_integer(),
          t_3_time_difference_in_days: number(),
          t_3_total_population: non_neg_integer(),
          t_3_population_increase: non_neg_integer(),
          t_3_population_increase_by_founded: non_neg_integer(),
          t_3_population_increase_by_conquered: non_neg_integer(),
          t_3_population_decrease: non_neg_integer(),
          t_3_population_decrease_by_conquered: non_neg_integer(),
          t_3_population_decrease_by_destroyed: non_neg_integer(),
          t_3_total_villages: non_neg_integer(),
          t_3_n_villages_with_population_increase: non_neg_integer(),
          t_3_n_villages_with_population_decrease: non_neg_integer(),
          t_3_n_villages_with_population_stuck: non_neg_integer(),
          t_3_new_village_founded: non_neg_integer(),
          t_3_new_village_conquered: non_neg_integer(),
          t_3_lost_village_conquered: non_neg_integer(),
          t_3_lost_village_destroyed: non_neg_integer(),
          ## Today - 4 sample
          # Represents a boolean value as 0 or 1
          t_4_has_data?: non_neg_integer(),
          # Represents a boolean value as 0 or 1
          t_4_has_increase?: non_neg_integer(),
          t_4_time_difference_in_days: number(),
          t_4_total_population: non_neg_integer(),
          t_4_population_increase: non_neg_integer(),
          t_4_population_increase_by_founded: non_neg_integer(),
          t_4_population_increase_by_conquered: non_neg_integer(),
          t_4_population_decrease: non_neg_integer(),
          t_4_population_decrease_by_conquered: non_neg_integer(),
          t_4_population_decrease_by_destroyed: non_neg_integer(),
          t_4_total_villages: non_neg_integer(),
          t_4_n_villages_with_population_increase: non_neg_integer(),
          t_4_n_villages_with_population_decrease: non_neg_integer(),
          t_4_n_villages_with_population_stuck: non_neg_integer(),
          t_4_new_village_founded: non_neg_integer(),
          t_4_new_village_conquered: non_neg_integer(),
          t_4_lost_village_conquered: non_neg_integer(),
          t_4_lost_village_destroyed: non_neg_integer(),
          ## Today - 5 sample
          # Represents a boolean value as 0 or 1
          t_5_has_data?: non_neg_integer(),
          # Represents a boolean value as 0 or 1
          t_5_has_increase?: non_neg_integer(),
          t_5_time_difference_in_days: number(),
          t_5_total_population: non_neg_integer(),
          t_5_population_increase: non_neg_integer(),
          t_5_population_increase_by_founded: non_neg_integer(),
          t_5_population_increase_by_conquered: non_neg_integer(),
          t_5_population_decrease: non_neg_integer(),
          t_5_population_decrease_by_conquered: non_neg_integer(),
          t_5_population_decrease_by_destroyed: non_neg_integer(),
          t_5_total_villages: non_neg_integer(),
          t_5_n_villages_with_population_increase: non_neg_integer(),
          t_5_n_villages_with_population_decrease: non_neg_integer(),
          t_5_n_villages_with_population_stuck: non_neg_integer(),
          t_5_new_village_founded: non_neg_integer(),
          t_5_new_village_conquered: non_neg_integer(),
          t_5_lost_village_conquered: non_neg_integer(),
          t_5_lost_village_destroyed: non_neg_integer(),
          ## Today - 6 sample
          # Represents a boolean value as 0 or 1
          t_6_has_data?: non_neg_integer(),
          # Represents a boolean value as 0 or 1
          t_6_has_increase?: non_neg_integer(),
          t_6_time_difference_in_days: number(),
          t_6_total_population: non_neg_integer(),
          t_6_population_increase: non_neg_integer(),
          t_6_population_increase_by_founded: non_neg_integer(),
          t_6_population_increase_by_conquered: non_neg_integer(),
          t_6_population_decrease: non_neg_integer(),
          t_6_population_decrease_by_conquered: non_neg_integer(),
          t_6_population_decrease_by_destroyed: non_neg_integer(),
          t_6_total_villages: non_neg_integer(),
          t_6_n_villages_with_population_increase: non_neg_integer(),
          t_6_n_villages_with_population_decrease: non_neg_integer(),
          t_6_n_villages_with_population_stuck: non_neg_integer(),
          t_6_new_village_founded: non_neg_integer(),
          t_6_new_village_conquered: non_neg_integer(),
          t_6_lost_village_conquered: non_neg_integer(),
          t_6_lost_village_destroyed: non_neg_integer(),
          ## Today - 7 sample
          # Represents a boolean value as 0 or 1
          t_7_has_data?: non_neg_integer(),
          # Represents a boolean value as 0 or 1
          t_7_has_increase?: non_neg_integer(),
          t_7_time_difference_in_days: number(),
          t_7_total_population: non_neg_integer(),
          t_7_population_increase: non_neg_integer(),
          t_7_population_increase_by_founded: non_neg_integer(),
          t_7_population_increase_by_conquered: non_neg_integer(),
          t_7_population_decrease: non_neg_integer(),
          t_7_population_decrease_by_conquered: non_neg_integer(),
          t_7_population_decrease_by_destroyed: non_neg_integer(),
          t_7_total_villages: non_neg_integer(),
          t_7_n_villages_with_population_increase: non_neg_integer(),
          t_7_n_villages_with_population_decrease: non_neg_integer(),
          t_7_n_villages_with_population_stuck: non_neg_integer(),
          t_7_new_village_founded: non_neg_integer(),
          t_7_new_village_conquered: non_neg_integer(),
          t_7_lost_village_conquered: non_neg_integer(),
          t_7_lost_village_destroyed: non_neg_integer()
        }

  @impl true
  def options(), do: {"medusa_pred_input", ".c6bert"}

  @impl true
  def to_format(medusa_pred_input),
    do: :erlang.term_to_binary(medusa_pred_input, [:compressed, :deterministic])

  @impl true
  def from_format(encoded_medusa_pred_input),
    do: :erlang.binary_to_term(encoded_medusa_pred_input)

  @impl true
  @spec run(root_folder :: String.t(), server_id :: TTypes.server_id(), target_date :: Date.t()) ::
          :ok | {:error, any()}
  def run(root_folder, server_id, target_date) do
    with(
      {:a, {:ok, snapshot}} <-
        {:a, Collector.Feed.open(root_folder, server_id, target_date, Collector.Snapshot)},
      {:b, {:ok, agg_players}} <-
        {:b, Collector.Feed.open(root_folder, server_id, target_date, Collector.AggPlayers)},
      {:c, {:ok, agg_server}} <-
        {:c, Collector.Feed.open(root_folder, server_id, target_date, Collector.AggServer)}
    ) do
      uniq_snapshot = Enum.uniq_by(snapshot, & &1.player_id)
      player_ids = for row <- uniq_snapshot, do: row.player_id
      uniq_agg_players = for agg_p <- agg_players, agg_p.player_id in player_ids, do: agg_p

      # We should have the same players in both feeds
      bundle =
        Enum.zip(
          Enum.sort_by(uniq_snapshot, & &1.player_id),
          Enum.sort_by(uniq_agg_players, & &1.player_id)
        )

      target_dt = DateTime.new!(target_date, ~T[00:00:00.000])

      medusa_pred_input =
        for {s, agg_p} <- bundle, do: process(server_id, target_dt, s, agg_p, agg_server)

      Collector.Feed.store(root_folder, server_id, target_date, medusa_pred_input, __MODULE__)
    else
      {:a, {:error, reason}} -> {:error, {"Unable to open snapshot", reason}}
      {:b, {:error, reason}} -> {:error, {"Unable to open agg_players", reason}}
      {:c, {:error, reason}} -> {:error, {"Unable to open agg_server", reason}}
    end
  end

  @spec process(
          server_id :: TTypes.server_id(),
          target_dt :: DateTime.t(),
          snapshot :: Collector.Snapshot.t(),
          agg_player :: Collector.AggPlayers.t(),
          agg_server :: Collector.AggServer.t()
        ) :: t()
  def process(server_id, target_dt, snapshot, agg_player, agg_server)
      when snapshot.player_id == agg_player.player_id do
    [last | rest] = Enum.sort_by(agg_player.increment, & &1.target_dt, {:desc, Date})
    increase? = length(rest) != 0

    target_date = DateTime.to_date(target_dt)

    t = %__MODULE__{
      target_dt: target_dt,
      server_id: server_id,
      player_id: snapshot.player_id,
      has_alliance?: if(snapshot.alliance_server_id == 0, do: 0, else: 1),
      # AggServer
      server_days_from_start: Date.diff(target_date, agg_server.estimated_starting_date),
      has_speed?: if(agg_server.speed, do: 1, else: 0),
      speed: if(agg_server.speed, do: agg_server.speed, else: 0),
      # AggPlayers
      player_days_from_start: Date.diff(target_date, agg_player.estimated_starting_date),
      estimated_tribe: agg_player.estimated_tribe,
      ## Today's data
      t_has_increase?: if(increase?, do: 1, else: 0),
      t_total_population: last.total_population,
      t_population_increase: if(increase?, do: last.population_increase, else: 0),
      t_population_increase_by_founded:
        if(increase?, do: last.population_increase_by_founded, else: 0),
      t_population_increase_by_conquered:
        if(increase?, do: last.population_increase_by_conquered, else: 0),
      t_population_decrease: if(increase?, do: last.population_decrease, else: 0),
      t_population_decrease_by_conquered:
        if(increase?, do: last.population_decrease_by_conquered, else: 0),
      t_population_decrease_by_destroyed:
        if(increase?, do: last.population_decrease_by_destroyed, else: 0),
      t_total_villages: last.total_villages,
      t_n_villages_with_population_increase:
        if(increase?, do: last.n_villages_with_population_increase, else: 0),
      t_n_villages_with_population_decrease:
        if(increase?, do: last.n_villages_with_population_decrease, else: 0),
      t_n_villages_with_population_stuck:
        if(increase?, do: last.n_villages_with_population_stuck, else: 0),
      t_new_village_founded: if(increase?, do: last.new_village_founded, else: 0),
      t_new_village_conquered: if(increase?, do: last.new_village_conquered, else: 0),
      t_lost_village_conquered: if(increase?, do: last.lost_village_conquered, else: 0),
      t_lost_village_destroyed: if(increase?, do: last.lost_village_destroyed, else: 0)
    }

    update_t_n_samples(t, target_dt, rest)
  end

  def update_t_n_samples(t, _target_dt, []), do: t

  def update_t_n_samples(t, target_dt, [n_sample | rest]) do
    update_t_n_samples(t, target_dt, 1, n_sample, rest)
  end

  def update_t_n_samples(t, _target_dt, 8, _n_sample, _rest) do
    t
  end

  def update_t_n_samples(t, target_dt, n, n_sample, []) do
    updated_fields = %{
      ta("t_#{n}_has_data?") => 1,
      ta("t_#{n}_has_increase?") => 0,
      ta("t_#{n}_time_difference_in_days") =>
        DateTime.diff(target_dt, n_sample.target_dt) / (3600 * 24),
      ta("t_#{n}_total_population") => n_sample.total_population,
      ta("t_#{n}_total_villages") => n_sample.total_villages
    }

    struct!(t, updated_fields)
  end

  def update_t_n_samples(t, target_dt, n, n_sample, [n2_sample | rest]) do
    updated_fields = %{
      ta("t_#{n}_has_data?") => 1,
      ta("t_#{n}_has_increase?") => 1,
      ta("t_#{n}_time_difference_in_days") =>
        DateTime.diff(target_dt, n_sample.target_dt) / (3600 * 24),
      ta("t_#{n}_total_population") => n_sample.total_population,
      ta("t_#{n}_population_increase") => n_sample.population_increase,
      ta("t_#{n}_population_increase_by_founded") => n_sample.population_increase_by_founded,
      ta("t_#{n}_population_increase_by_conquered") => n_sample.population_increase_by_conquered,
      ta("t_#{n}_population_decrease") => n_sample.population_decrease,
      ta("t_#{n}_population_decrease_by_conquered") => n_sample.population_decrease_by_conquered,
      ta("t_#{n}_population_decrease_by_destroyed") => n_sample.population_decrease_by_destroyed,
      ta("t_#{n}_total_villages") => n_sample.total_villages,
      ta("t_#{n}_n_villages_with_population_increase") =>
        n_sample.n_villages_with_population_increase,
      ta("t_#{n}_n_villages_with_population_decrease") =>
        n_sample.n_villages_with_population_decrease,
      ta("t_#{n}_n_villages_with_population_stuck") => n_sample.n_villages_with_population_stuck,
      ta("t_#{n}_new_village_founded") => n_sample.new_village_founded,
      ta("t_#{n}_new_village_conquered") => n_sample.new_village_conquered,
      ta("t_#{n}_lost_village_conquered") => n_sample.lost_village_conquered,
      ta("t_#{n}_lost_village_destroyed") => n_sample.lost_village_destroyed
    }

    new_t = struct!(t, updated_fields)
    update_t_n_samples(new_t, target_dt, n + 1, n2_sample, rest)
  end

  # alias
  defp ta(s), do: String.to_atom(s)
end
