module Database.PostgreSQL.PQTypes.ExtrasOptions
  ( ExtrasOptions(..)
  , defaultExtrasOptions
  , ObjectsValidationMode(..)
  ) where

data ExtrasOptions =
    ExtrasOptions
    { eoCommitAfterEachMigration :: Bool
      -- ^ Run commit after every migration.
    , eoEnforcePKs               :: Bool
      -- ^ Validate that every handled table has a primary key
    , eoObjectsValidationMode    :: !ObjectsValidationMode
      -- ^ Validation mode for unknown tables and composite types.
    , eoAllowHigherTableVersions :: !Bool
      -- ^ Whether to allow tables in the database to have higher versions than
      -- the one in the code definition.
    } deriving Eq

defaultExtrasOptions :: ExtrasOptions
defaultExtrasOptions = ExtrasOptions
  { eoCommitAfterEachMigration = False
  , eoEnforcePKs               = False
  , eoObjectsValidationMode    = DontAllowUnknownObjects
  , eoAllowHigherTableVersions = False
  }

data ObjectsValidationMode = AllowUnknownObjects | DontAllowUnknownObjects
  deriving Eq
