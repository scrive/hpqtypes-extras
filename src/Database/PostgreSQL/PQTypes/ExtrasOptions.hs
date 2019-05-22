module Database.PostgreSQL.PQTypes.ExtrasOptions
  ( ExtrasOptions(..)
  , defaultExtrasOptions
  ) where

data ExtrasOptions =
    ExtrasOptions
    { eoForceCommit :: Bool
      -- ^ Force commit after every migration
    , eoEnforcePKs :: Bool
      -- ^ Validate that every handled table has a primary key
    } deriving Eq

defaultExtrasOptions :: ExtrasOptions
defaultExtrasOptions = ExtrasOptions
  { eoForceCommit = False
  , eoEnforcePKs  = False
  }
