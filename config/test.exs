import Config

# We don't run a server during test. If one is required,
# you can enable the server option below.
config :front, Front.Endpoint,
  http: [ip: {127, 0, 0, 1}, port: 4002],
  secret_key_base: "sqjleJ6vBc26GwangMiqo6ZJEB8A1zgvT4B935DjXgkbZc/K0/hymWixEwLXVcJB",
  server: false
