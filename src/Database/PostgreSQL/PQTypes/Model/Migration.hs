module Database.PostgreSQL.PQTypes.Model.Migration (
    Migration(..)
  )  where

import Data.Int

import Database.PostgreSQL.PQTypes.Model.Table

-- | Migration object.
data Migration m = Migration {
  -- | The table you're migrating.
  mgrTable :: Table
  -- | The version you're migrating from (you don't specify what
  -- version you migrate TO, because version is always increased by 1,
  -- so if 'mgrFrom' is 2, that means that after that migration is run,
  -- table version will equal 3
, mgrFrom  :: Int32
  -- | Actual body of a migration
, mgrDo    :: m ()
}
