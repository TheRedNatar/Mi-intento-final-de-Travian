defmodule TDB.Server do
  use Ecto.Schema


  @primary_key {:server_id, :string, []}

  schema "servers" do
    field :url, :string
    field :init_date, :date
    
    field :country , :string, size: 10
    field :worldId , :string
    field :speed , :integer
    field :appId , :string, null: true
    field :version , :string, null: true
    
    field :height , :integer
    field :bottom , :integer
    field :left , :integer
    field :right , :integer
    field :top , :integer
    field :width , :integer
    
    field :adventuresDecay , :boolean
    field :allianceBanner , :boolean
    field :allianceBonus , :boolean
    field :boostedStart , :boolean
    field :cities , :boolean
    field :contextHelp , :boolean
    field :factions , :boolean
    field :healHeroOnLevelUp , :boolean
    field :hideFoolsArtifacts , :boolean
    field :lockingRegionsAgain , :boolean
    field :multi_language , :boolean
    field :progressiveTasks , :boolean
    field :rearrangeBuildings , :boolean
    field :resourcesInHeroBag , :boolean
    field :sittingOnlyFriends , :boolean
    field :territory , :boolean
    field :travelOverTheWorldEdge , :boolean
    field :tribesEgyptiansAndHuns , :boolean
    field :useAdventureSpawnTime , :boolean
    field :vacationMode , :boolean

    timestamps()
  end
end