{-# LANGUAGE CPP #-}
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
    MigrationAction(..),
    Migration(..),
    isStandardMigration, isDropTableMigration
  )  where

import Data.Int

import Database.PostgreSQL.PQTypes.Model.Index
import Database.PostgreSQL.PQTypes.Model.Table
import Database.PostgreSQL.PQTypes.SQL.Raw

-- | Migration action to run, either an arbitrary 'MonadDB' action, or
-- something more fine-grained.
data MigrationAction m =

  -- | Standard migration, i.e. an arbitrary 'MonadDB' action.
  StandardMigration (m ())

  -- | Drop table migration. Parameter is the drop table mode
  -- (@RESTRICT@/@CASCADE@). The 'Migration' record holds the name of
  -- the table to drop.
  | DropTableMigration DropTableMode

  -- | Migration for creating an index concurrently.
  | CreateIndexConcurrentlyMigration
      (RawSQL ()) -- ^ Table name
      TableIndex  -- ^ Index

  -- | Migration for dropping an index concurrently.
  | DropIndexConcurrentlyMigration
      (RawSQL ()) -- ^ Table name
      TableIndex  -- ^ Index

-- | Migration object.
data Migration m =
  Migration {
  -- | The name of the table you're migrating.
  mgrTableName :: RawSQL ()
  -- | The version you're migrating *from* (you don't specify what
  -- version you migrate TO, because version is always increased by 1,
  -- so if 'mgrFrom' is 2, that means that after that migration is run,
  -- table version will equal 3
, mgrFrom   :: Int32
  -- | Migration action.
, mgrAction :: MigrationAction m
  }

isStandardMigration :: Migration m -> Bool
isStandardMigration Migration{..} =
  case mgrAction of
    StandardMigration{}                -> True
    DropTableMigration{}               -> False
    CreateIndexConcurrentlyMigration{} -> False
    DropIndexConcurrentlyMigration{}   -> False

isDropTableMigration :: Migration m -> Bool
isDropTableMigration Migration{..} =
  case mgrAction of
    StandardMigration{}                -> False
    DropTableMigration{}               -> True
    CreateIndexConcurrentlyMigration{} -> False
    DropIndexConcurrentlyMigration{}   -> False
