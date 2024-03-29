import Config

config :logger,
       :console,
       # level: System.get_env("MITRAVIAN__LOGGER_LEVEL", "info") |> String.to_atom(),
       format: "$node $date $time [$level] ($metadata) $message\n",
       metadata: [:mfa]

config :mnesia,
  dir: System.get_env("MITRAVIAN__MNESIA_DIR", "/tmp/mnesia") |> String.to_charlist()

config :connector,
  node_to_ping: System.get_env("MITRAVIAN__NODE_TO_PING", "legati@somenode") |> String.to_atom()

config :collector,
  root_folder: System.get_env("MITRAVIAN_ROOTFOLDER", "/tmp/travian_folder"),
  attemps: System.get_env("MITRAVIAN__COLLECTOR_ATTEMPS", "3") |> String.to_integer(),
  min:
    System.get_env("MITRAVIAN__COLLECTOR_MIN", "1")
    |> String.to_integer()
    |> then(fn x -> x * 1000 end),
  max:
    System.get_env("MITRAVIAN__COLLECTOR_MAX", "120")
    |> String.to_integer()
    |> then(fn x -> x * 1000 end),
  stages:
    System.get_env("MITRAVIAN__COLLECTOR_STAGES", "2")
    |> String.to_integer(),
  collection_hour: Time.new!(3, 0, 0),

  # Mnesia related
  retention_period_api_map_sql:
    System.get_env("MITRAVIAN__MNESIA_RETENTION_PERIOD_API_MAP_SQL", "7") |> String.to_integer(),

  # Collector Medusa Model Options
  model_dir:
    System.get_env("MITRAVIAN__MEDUSA_MODELDIR", "/home/jorge/Proyectos/TheRedNatar/MedusaPY"),
  model: System.get_env("MITRAVIAN__MEDUSA_MODEL", "trained_models/medusa_model.pkl"),
  py_version: System.get_env("MITRAVIAN__MEDUSA_PY_VERSION", "3.7"),
  py_env: System.get_env("MITRAVIAN__MEDUSA_PY_ENV", "medusa_env/lib/python3.7/site-packages"),
  server: System.get_env("MITRAVIAN__MEDUSA_SERVER", "app.py")

if config_env() == :prod do
  import Config

  # The secret key base is used to sign/encrypt cookies and other secrets.
  # A default value is used in config/dev.exs and config/test.exs but you
  # want to use a different value for prod and you most likely don't want
  # to check this value into version control, so we use an environment
  # variable instead.
  secret_key_base =
    System.get_env("SECRET_KEY_BASE") ||
      raise """
      environment variable SECRET_KEY_BASE is missing.
      You can generate one by calling: mix phx.gen.secret
      """

  config :front, Front.Endpoint,
    http: [
      # Enable IPv6 and bind on all interfaces.
      # Set it to  {0, 0, 0, 0, 0, 0, 0, 1} for local network only access.
      ip: {0, 0, 0, 0, 0, 0, 0, 0},
      port: String.to_integer(System.get_env("PORT") || "4000")
    ],
    secret_key_base: secret_key_base

  # ## Using releases
  #
  # If you are doing OTP releases, you need to instruct Phoenix
  # to start each relevant endpoint:
  #
  #     config :front, Front.Endpoint, server: true
  #
  # Then you can assemble a release by calling `mix release`.
  # See `mix help release` for more information.

  # ## SSL Support
  #
  # To get SSL working, you will need to add the `https` key
  # to your endpoint configuration:
  #
  #     config :front, Front.Endpoint,
  #       https: [
  #         ...,
  #         port: 443,
  #         cipher_suite: :strong,
  #         keyfile: System.get_env("SOME_APP_SSL_KEY_PATH"),
  #         certfile: System.get_env("SOME_APP_SSL_CERT_PATH")
  #       ]
  #
  # The `cipher_suite` is set to `:strong` to support only the
  # latest and more secure SSL ciphers. This means old browsers
  # and clients may not be supported. You can set it to
  # `:compatible` for wider support.
  #
  # `:keyfile` and `:certfile` expect an absolute path to the key
  # and cert in disk or a relative path inside priv, for example
  # "priv/ssl/server.key". For all supported SSL configuration
  # options, see https://hexdocs.pm/plug/Plug.SSL.html#configure/1
  #
  # We also recommend setting `force_ssl` in your endpoint, ensuring
  # no data is ever sent via http, always redirecting to https:
  #
  #     config :front, Front.Endpoint,
  #       force_ssl: [hsts: true]
  #
  # Check `Plug.SSL` for all available options in `force_ssl`.

  # ## Configuring the mailer
  #
  # In production you need to configure the mailer to use a different adapter.
  # Also, you may need to configure the Swoosh API client of your choice if you
  # are not using SMTP. Here is an example of the configuration:
  #
  #     config :front, Front.Mailer,
  #       adapter: Swoosh.Adapters.Mailgun,
  #       api_key: System.get_env("MAILGUN_API_KEY"),
  #       domain: System.get_env("MAILGUN_DOMAIN")
  #
  # For this example you need include a HTTP client required by Swoosh API client.
  # Swoosh supports Hackney and Finch out of the box:
  #
  #     config :swoosh, :api_client, Swoosh.ApiClient.Hackney
  #
  # See https://hexdocs.pm/swoosh/Swoosh.html#module-installation for details.

  # The secret key base is used to sign/encrypt cookies and other secrets.
  # A default value is used in config/dev.exs and config/test.exs but you
  # want to use a different value for prod and you most likely don't want
  # to check this value into version control, so we use an environment
  # variable instead.
  secret_key_base =
    System.get_env("MITRAVIAN__SECRET_KEY_BASE") ||
      raise """
      environment variable SECRET_KEY_BASE is missing.
      You can generate one by calling: mix phx.gen.secret
      """

  config :front, Front.Endpoint,
    http: [
      # Enable IPv6 and bind on all interfaces.
      # Set it to  {0, 0, 0, 0, 0, 0, 0, 1} for local network only access.
      # ip: {0, 0, 0, 0, 0, 0, 0, 0},
      # ip: System.get_env("MITRAVIAN__FRONT_IPv6", "0:0:0:0:0:0:0:0") |> String.split(":") |> Enum.map(&String.to_integer/1) |> List.to_tuple(),
      ip:
        System.get_env("MITRAVIAN__FRONT_IPv4", "0:0:0:0")
        |> String.split(":")
        |> Enum.map(&String.to_integer/1)
        |> List.to_tuple(),
      port: String.to_integer(System.get_env("MITRAVIAN__FRONT_PORT") || "4000")
    ],
    secret_key_base: secret_key_base,
    server: true

  # ## Using releases
  #
  # If you are doing OTP releases, you need to instruct Phoenix
  # to start each relevant endpoint:
  #
  #     config :front, Front.Endpoint, server: true
  #
  # Then you can assemble a release by calling `mix release`.
  # See `mix help release` for more information.

  # ## Configuring the mailer
  #
  # In production you need to configure the mailer to use a different adapter.
  # Also, you may need to configure the Swoosh API client of your choice if you
  # are not using SMTP. Here is an example of the configuration:
  #
  #     config :front, Front.Mailer,
  #       adapter: Swoosh.Adapters.Mailgun,
  #       api_key: System.get_env("MAILGUN_API_KEY"),
  #       domain: System.get_env("MAILGUN_DOMAIN")
  #
  # For this example you need include a HTTP client required by Swoosh API client.
  # Swoosh supports Hackney and Finch out of the box:
  #
  #     config :swoosh, :api_client, Swoosh.ApiClient.Hackney
  #
  # See https://hexdocs.pm/swoosh/Swoosh.html#module-installation for details.
end
