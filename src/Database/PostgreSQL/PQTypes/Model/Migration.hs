{- |

Using migrations is fairly easy. After you've defined the lists of
migrations and tables, just run
'Database.PostgreSQL.PQTypes.Checks.migrateDatabase':

@
tables :: [Table]
tables = ...

migrations :: [Migration]
migrations = ...

migrateDatabase options extensions domains tables migrations
@

Migrations are run strictly in the order specified in the migrations
list, starting with the first migration for which the corresponding
table in the DB has the version number equal to the 'mgrFrom' field of
the migration.

-}

module Database.PostgreSQL.PQTypes.Model.Migration (
    DropTableMode(..),
    MigrationType(..),
    Migration(..),
    isStandardMigration, isDropTableMigration
  )  where

import Data.Int

import Database.PostgreSQL.PQTypes.Model.Table

-- | Migration type and additional associated data.
data MigrationType m =

  -- | Standard migration. Associated data is the 'MonadDB' action to run.
  StandardMigration (m ())

  -- | Drop table migration. Associated data is the drop table mode
  -- (@RESTRICT@/@CASCADE@).
  | DropTableMigration DropTableMode

-- | Migration object.
data Migration m =
  Migration {
  -- | The table you're migrating.
  mgrTable :: Table
  -- | The version you're migrating from (you don't specify what
  -- version you migrate TO, because version is always increased by 1,
  -- so if 'mgrFrom' is 2, that means that after that migration is run,
  -- table version will equal 3
, mgrFrom  :: Int32
  -- | Migration type and additional type-specific associated data.
, mgrType    :: MigrationType m
  }

isStandardMigration :: Migration m -> Bool
isStandardMigration Migration{..} =
  case mgrType of
    StandardMigration _  -> True
    DropTableMigration _ -> False

isDropTableMigration :: Migration m -> Bool
isDropTableMigration Migration{..} =
  case mgrType of
    StandardMigration _  -> False
    DropTableMigration _ -> True
