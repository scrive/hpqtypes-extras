module Database.PostgreSQL.PQTypes.ExtrasOptions
  ( ExtrasOptions (..)
  , defaultExtrasOptions
  , ObjectsValidationMode (..)
  ) where

data ExtrasOptions
  = ExtrasOptions
  { eoLockTimeoutMs :: !Int
  , eoLockFailureBackoffSecs :: !Int
  , eoLockAttempts :: !Int
  , eoEnforcePKs :: !Bool
  -- ^ Validate that every handled table has a primary key
  , eoObjectsValidationMode :: !ObjectsValidationMode
  -- ^ Validation mode for unknown tables and composite types.
  , eoAllowHigherTableVersions :: !Bool
  -- ^ Whether to allow tables in the database to have higher versions than
  -- the one in the code definition.
  , eoCheckForeignKeysIndexes :: !Bool
  -- ^ Check if all foreign keys have indexes.
  , eoCheckOverlappingIndexes :: !Bool
  -- ^ Check if some indexes are redundant
  }
  deriving (Eq)

defaultExtrasOptions :: ExtrasOptions
defaultExtrasOptions =
  ExtrasOptions
    { eoLockTimeoutMs = 3000
    , eoLockFailureBackoffSecs = 30
    , eoLockAttempts = 5
    , eoEnforcePKs = False
    , eoObjectsValidationMode = DontAllowUnknownObjects
    , eoAllowHigherTableVersions = False
    , eoCheckForeignKeysIndexes = False
    , eoCheckOverlappingIndexes = False
    }

data ObjectsValidationMode = AllowUnknownObjects | DontAllowUnknownObjects
  deriving (Eq)
