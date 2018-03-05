module Database.PostgreSQL.PQTypes.ExtrasOptions (
  ExtrasOptions(..)
  ) where
import Data.Default

data ExtrasOptions =
    ExtrasOptions
    {
      eoForceCommit :: Bool
    -- ^ Force commit after every migration
    , eoEnforcePKs :: Bool
    -- ^ Validate that every handled table has a primary key
    } deriving Eq

instance Default ExtrasOptions where
    def = ExtrasOptions False False
