module Main
  where

import Control.Exception.Lifted as E
import Control.Monad.IO.Class
import Control.Monad.Trans.Control

import Data.Either
import Data.Monoid
import Prelude
import qualified Data.Text as T
import Data.Typeable
import Data.UUID.Types

import Database.PostgreSQL.PQTypes
import Database.PostgreSQL.PQTypes.Checks
import Database.PostgreSQL.PQTypes.Model.ColumnType
import Database.PostgreSQL.PQTypes.Model.CompositeType
import Database.PostgreSQL.PQTypes.Model.ForeignKey
import Database.PostgreSQL.PQTypes.Model.Index
import Database.PostgreSQL.PQTypes.Model.Migration
import Database.PostgreSQL.PQTypes.Model.PrimaryKey
import Database.PostgreSQL.PQTypes.Model.Table
import Database.PostgreSQL.PQTypes.SQL.Builder
import Log
import Log.Backend.StandardOutput

import Test.Tasty
import Test.Tasty.HUnit
import Test.Tasty.Options

data ConnectionString = ConnectionString String
  deriving Typeable

instance IsOption ConnectionString where
  defaultValue = ConnectionString
    -- For GitHub Actions CI
    "host=postgres user=postgres password=postgres"
  parseValue   = Just . ConnectionString
  optionName   = return "connection-string"
  optionHelp   = return "Postgres connection string"

-- Simple example schemata inspired by the one in
-- <  http://www.databaseanswers.org/data_models/bank_robberies/index.htm>
--
-- Schema 1: Bank robberies, tables:
--           bank, bad_guy, robbery, participated_in_robbery, witness,
--           witnessed_robbery
-- Schema 2: Witnesses go into witness protection program,
--           (some) bad guys get arrested:
--           drop tables witness and witnessed_robbery,
--           add table under_arrest
-- Schema 3: Bad guys get their prison sentences:
--           drop table under_arrest
--           add table prison_sentence
-- Schema 4: New 'cash' column for the 'bank' table.
-- Schema 5: Create a new table 'flash',
--           drop the 'cash' column from the 'bank' table,
--           drop table 'flash'.
-- Cleanup:  drop everything

tableBankSchema1 :: Table
tableBankSchema1 =
  tblTable
  { tblName = "bank"
  , tblVersion = 1
  , tblColumns =
    [ tblColumn { colName = "id",       colType = UuidT
                , colNullable = False
                , colDefault = Just "gen_random_uuid()" }
    , tblColumn { colName = "name",     colType = TextT
                , colNullable = False }
    , tblColumn { colName = "location", colType = TextT
                , colNullable = False }
    ]
  , tblPrimaryKey = pkOnColumn "id"
  }

tableBankSchema2 :: Table
tableBankSchema2 = tableBankSchema1

tableBankSchema3 :: Table
tableBankSchema3 = tableBankSchema2

tableBankMigration4 :: (MonadDB m) => Migration m
tableBankMigration4 = Migration
  { mgrTableName = tblName tableBankSchema3
  , mgrFrom      = 1
  , mgrAction    = StandardMigration $ do
      runQuery_ $ sqlAlterTable (tblName tableBankSchema3) [
        sqlAddColumn $ tblColumn
          { colName = "cash"
          , colType = IntegerT
          , colNullable = False
          , colDefault = Just "0"
          }
        ]
  }

tableBankSchema4 :: Table
tableBankSchema4 = tableBankSchema3 {
    tblVersion = (tblVersion tableBankSchema3)  + 1
  , tblColumns = (tblColumns tableBankSchema3) ++ [
      tblColumn
      { colName = "cash", colType = IntegerT
      , colNullable = False
      , colDefault = Just "0"
      }
    ]
  }


tableBankMigration5fst :: (MonadDB m) => Migration m
tableBankMigration5fst = Migration
  { mgrTableName = tblName tableBankSchema3
  , mgrFrom      = 2
  , mgrAction    = StandardMigration $ do
      runQuery_ $ sqlAlterTable (tblName tableBankSchema4) [
        sqlDropColumn $ "cash"
        ]
  }

tableBankMigration5snd :: (MonadDB m) => Migration m
tableBankMigration5snd = Migration
  { mgrTableName = tblName tableBankSchema3
  , mgrFrom      = 3
  , mgrAction    = CreateIndexConcurrentlyMigration
                     (tblName tableBankSchema3)
                     (indexOnColumn "name")
  }

tableBankSchema5 :: Table
tableBankSchema5 = tableBankSchema4 {
    tblVersion = (tblVersion tableBankSchema4) + 2
  , tblColumns = filter (\c -> colName c /= "cash")
      (tblColumns tableBankSchema4)
  , tblIndexes = [indexOnColumn "name"]
  }

tableBadGuySchema1 :: Table
tableBadGuySchema1 =
  tblTable
  { tblName = "bad_guy"
  , tblVersion = 1
  , tblColumns =
    [ tblColumn { colName = "id",        colType = UuidT
                , colNullable = False
                , colDefault = Just "gen_random_uuid()" }
    , tblColumn { colName = "firstname", colType = TextT
                , colNullable = False }
    , tblColumn { colName = "lastname",  colType = TextT
                , colNullable = False }
    ]
  , tblPrimaryKey = pkOnColumn "id" }

tableBadGuySchema2 :: Table
tableBadGuySchema2 = tableBadGuySchema1

tableBadGuySchema3 :: Table
tableBadGuySchema3 = tableBadGuySchema2

tableBadGuySchema4 :: Table
tableBadGuySchema4 = tableBadGuySchema3

tableBadGuySchema5 :: Table
tableBadGuySchema5 = tableBadGuySchema4

tableRobberySchema1 :: Table
tableRobberySchema1 =
  tblTable
  { tblName = "robbery"
  , tblVersion = 1
  , tblColumns =
    [ tblColumn { colName = "id",      colType = UuidT
                , colNullable = False
                , colDefault = Just "gen_random_uuid()" }
    , tblColumn { colName = "bank_id", colType = UuidT
                , colNullable = False }
    , tblColumn { colName = "date",    colType = DateT
                , colNullable = False, colDefault = Just "now()" }
    ]
  , tblPrimaryKey  = pkOnColumn "id"
  , tblForeignKeys = [fkOnColumn "bank_id" "bank" "id"] }

tableRobberySchema2 :: Table
tableRobberySchema2 = tableRobberySchema1

tableRobberySchema3 :: Table
tableRobberySchema3 = tableRobberySchema2

tableRobberySchema4 :: Table
tableRobberySchema4 = tableRobberySchema3

tableRobberySchema5 :: Table
tableRobberySchema5 = tableRobberySchema4

tableParticipatedInRobberySchema1 :: Table
tableParticipatedInRobberySchema1 =
  tblTable
  { tblName = "participated_in_robbery"
  , tblVersion = 1
  , tblColumns =
    [ tblColumn { colName = "bad_guy_id", colType = UuidT
                , colNullable = False }
    , tblColumn { colName = "robbery_id", colType = UuidT
                , colNullable = False }
    ]
  , tblPrimaryKey  = pkOnColumns ["bad_guy_id", "robbery_id"]
  , tblForeignKeys = [fkOnColumn  "bad_guy_id" "bad_guy" "id"
                     ,fkOnColumn  "robbery_id" "robbery" "id"] }

tableParticipatedInRobberySchema2 :: Table
tableParticipatedInRobberySchema2 = tableParticipatedInRobberySchema1

tableParticipatedInRobberySchema3 :: Table
tableParticipatedInRobberySchema3 = tableParticipatedInRobberySchema2

tableParticipatedInRobberySchema4 :: Table
tableParticipatedInRobberySchema4 = tableParticipatedInRobberySchema3

tableParticipatedInRobberySchema5 :: Table
tableParticipatedInRobberySchema5 = tableParticipatedInRobberySchema4

tableWitnessName :: RawSQL ()
tableWitnessName = "witness"

tableWitnessSchema1 :: Table
tableWitnessSchema1 =
  tblTable
  { tblName = tableWitnessName
  , tblVersion = 1
  , tblColumns =
    [ tblColumn { colName = "id",        colType = UuidT
                , colNullable = False
                , colDefault = Just "gen_random_uuid()" }
    , tblColumn { colName = "firstname", colType = TextT
                , colNullable = False }
    , tblColumn { colName = "lastname",  colType = TextT
                , colNullable = False }
    ]
  , tblPrimaryKey = pkOnColumn "id" }

tableWitnessedRobberyName :: RawSQL ()
tableWitnessedRobberyName = "witnessed_robbery"

tableWitnessedRobberySchema1 :: Table
tableWitnessedRobberySchema1 =
  tblTable
  { tblName = tableWitnessedRobberyName
  , tblVersion = 1
  , tblColumns =
    [ tblColumn { colName = "witness_id", colType = UuidT
                , colNullable = False }
    , tblColumn { colName = "robbery_id", colType = UuidT
                , colNullable = False }
    ]
  , tblPrimaryKey  = pkOnColumns ["witness_id", "robbery_id"]
  , tblForeignKeys = [fkOnColumn  "witness_id" "witness" "id"
                     ,fkOnColumn  "robbery_id" "robbery" "id"] }

tableUnderArrestName :: RawSQL ()
tableUnderArrestName = "under_arrest"

tableUnderArrestSchema2 :: Table
tableUnderArrestSchema2 =
  tblTable
  { tblName = tableUnderArrestName
  , tblVersion = 1
  , tblColumns =
    [ tblColumn { colName = "bad_guy_id", colType = UuidT
                , colNullable = False }
    , tblColumn { colName = "robbery_id", colType = UuidT
                , colNullable = False }
    , tblColumn { colName = "court_date", colType = DateT
                , colNullable = False
                , colDefault  = Just "now()" }
    ]
  , tblPrimaryKey  = pkOnColumns ["bad_guy_id", "robbery_id"]
  , tblForeignKeys = [fkOnColumn  "bad_guy_id" "bad_guy" "id"
                     ,fkOnColumn  "robbery_id" "robbery" "id"] }

tablePrisonSentenceName :: RawSQL ()
tablePrisonSentenceName = "prison_sentence"

tablePrisonSentenceSchema3 :: Table
tablePrisonSentenceSchema3 =
  tblTable
  { tblName = tablePrisonSentenceName
  , tblVersion = 1
  , tblColumns =
    [ tblColumn { colName = "bad_guy_id", colType = UuidT
                , colNullable = False }
    , tblColumn { colName = "robbery_id", colType = UuidT
                , colNullable = False }
    , tblColumn { colName = "sentence_start"
                , colType = DateT
                , colNullable = False
                , colDefault  = Just "now()" }
    , tblColumn { colName = "sentence_length"
                , colType = IntegerT
                , colNullable = False
                , colDefault  = Just "6" }
    , tblColumn { colName = "prison_name"
                , colType = TextT
                , colNullable = False }
    ]
  , tblPrimaryKey  = pkOnColumns ["bad_guy_id", "robbery_id"]
  , tblForeignKeys = [fkOnColumn  "bad_guy_id" "bad_guy" "id"
                     ,fkOnColumn  "robbery_id" "robbery" "id"] }

tablePrisonSentenceSchema4 :: Table
tablePrisonSentenceSchema4 = tablePrisonSentenceSchema3

tablePrisonSentenceSchema5 :: Table
tablePrisonSentenceSchema5 = tablePrisonSentenceSchema4

tableFlashName :: RawSQL ()
tableFlashName = "flash"

tableFlash :: Table
tableFlash =
  tblTable
  { tblName = tableFlashName
  , tblVersion = 1
  , tblColumns =
    [ tblColumn { colName = "flash_id", colType = UuidT, colNullable = False }
    ]
  }

createTableMigration :: (MonadDB m) => Table -> Migration m
createTableMigration tbl = Migration
  { mgrTableName = tblName tbl
  , mgrFrom      = 0
  , mgrAction    = StandardMigration $ do
      createTable True tbl
  }

dropTableMigration :: (MonadDB m) => Table -> Migration m
dropTableMigration tbl = Migration
  { mgrTableName = tblName tbl
  , mgrFrom      = tblVersion tbl
  , mgrAction    = DropTableMigration DropTableCascade
  }

schema1Tables :: [Table]
schema1Tables = [ tableBankSchema1
                , tableBadGuySchema1
                , tableRobberySchema1
                , tableParticipatedInRobberySchema1
                , tableWitnessSchema1
                , tableWitnessedRobberySchema1
                ]

schema1Migrations :: (MonadDB m) => [Migration m]
schema1Migrations =
  [ createTableMigration tableBankSchema1
  , createTableMigration tableBadGuySchema1
  , createTableMigration tableRobberySchema1
  , createTableMigration tableParticipatedInRobberySchema1
  , createTableMigration tableWitnessSchema1
  , createTableMigration tableWitnessedRobberySchema1
  ]

schema2Tables :: [Table]
schema2Tables = [ tableBankSchema2
                , tableBadGuySchema2
                , tableRobberySchema2
                , tableParticipatedInRobberySchema2
                , tableUnderArrestSchema2
                ]

schema2Migrations :: (MonadDB m) => [Migration m]
schema2Migrations = schema1Migrations
                 ++ [ dropTableMigration   tableWitnessedRobberySchema1
                    , dropTableMigration   tableWitnessSchema1
                    , createTableMigration tableUnderArrestSchema2 ]

schema3Tables :: [Table]
schema3Tables = [ tableBankSchema3
                , tableBadGuySchema3
                , tableRobberySchema3
                , tableParticipatedInRobberySchema3
                , tablePrisonSentenceSchema3
                ]

schema3Migrations :: (MonadDB m) => [Migration m]
schema3Migrations = schema2Migrations
                 ++ [ dropTableMigration   tableUnderArrestSchema2
                    , createTableMigration tablePrisonSentenceSchema3 ]

schema4Tables :: [Table]
schema4Tables = [ tableBankSchema4
                , tableBadGuySchema4
                , tableRobberySchema4
                , tableParticipatedInRobberySchema4
                , tablePrisonSentenceSchema4
                ]

schema4Migrations :: (MonadDB m) => [Migration m]
schema4Migrations = schema3Migrations
                 ++ [ tableBankMigration4 ]

schema5Tables :: [Table]
schema5Tables = [ tableBankSchema5
                , tableBadGuySchema5
                , tableRobberySchema5
                , tableParticipatedInRobberySchema5
                , tablePrisonSentenceSchema5
                ]

schema5Migrations :: (MonadDB m) => [Migration m]
schema5Migrations = schema4Migrations
                 ++ [ createTableMigration tableFlash
                    , tableBankMigration5fst
                    , tableBankMigration5snd
                    , dropTableMigration tableFlash
                    ]

schema6Tables :: [Table]
schema6Tables =
          [ tableBankSchema1
          , tableBadGuySchema1
          , tableRobberySchema1
          , tableParticipatedInRobberySchema1
              { tblVersion = (tblVersion tableParticipatedInRobberySchema1) + 1,
                tblPrimaryKey = Nothing }
          , tableWitnessSchema1
          , tableWitnessedRobberySchema1
          ]

schema6Migrations :: (MonadDB m) => Migration m
schema6Migrations =
    Migration
    { mgrTableName = tblName tableParticipatedInRobberySchema1
    , mgrFrom = tblVersion tableParticipatedInRobberySchema1
    , mgrAction =
      StandardMigration $ do
        runQuery_ $ ("ALTER TABLE participated_in_robbery DROP CONSTRAINT \
                     \pk__participated_in_robbery" :: RawSQL ())
    }


type TestM a = DBT (LogT IO) a

createTablesSchema1 :: (String -> TestM ()) -> TestM ()
createTablesSchema1 step = do
  let extensions    = ["pgcrypto"]
      composites    = []
      domains       = []
  step "Creating the database (schema version 1)..."
  migrateDatabase defaultExtrasOptions extensions domains
    composites schema1Tables schema1Migrations
  checkDatabase defaultExtrasOptions composites domains schema1Tables

testDBSchema1 :: (String -> TestM ()) -> TestM ([UUID], [UUID])
testDBSchema1 step = do
  step "Running test queries (schema version 1)..."

  -- Populate the 'bank' table.
  runQuery_ . sqlInsert "bank" $ do
    sqlSetList "name" ["HSBC" :: T.Text, "Swedbank", "Nordea", "Citi"
                      ,"Wells Fargo"]
    sqlSetList "location" ["13 Foo St., Tucson, AZ, USa" :: T.Text
                          , "18 Bargatan, Stockholm, Sweden"
                          , "23 Baz Lane, Liverpool, UK"
                          , "2/3 Quux Ave., Milton Keynes, UK"
                          , "6600 Sunset Blvd., Los Angeles, CA, USA"]
    sqlResult "id"
  (bankIds :: [UUID]) <- fetchMany runIdentity
  liftIO $ assertEqual "INSERT into 'bank' table" 5 (length bankIds)

  -- Try to insert with existing ID to check that ON CONFLICT works properly
  let bankId = head bankIds
      name = "Santander" :: T.Text
      location = "Spain" :: T.Text

  runQuery_ . sqlInsert "bank" $ do
    sqlSet "id" bankId
    sqlSet "name" name
    sqlSet "location" location
    sqlOnConflictOnColumns ["id"] . sqlUpdate "" $ do
      sqlSet "name" name
      sqlSet "location" location
  runQuery_ . sqlSelect "bank" $ do
    sqlResult "name"
    sqlResult "location"
    sqlWhereEq "id" bankId
  details1 <- fetchOne id
  liftIO $ assertEqual "INSERT ON CONFLICT updates" (name, location) details1

  runQuery_ . sqlInsert "bank" $ do
    sqlSet "id" bankId
    sqlSet "name" ("" :: T.Text)
    sqlSet "location" ("" :: T.Text)
    sqlOnConflictDoNothing
  runQuery_ . sqlSelect "bank" $ do
    sqlResult "name"
    sqlResult "location"
    sqlWhereEq "id" bankId
  details3 <- fetchOne id
  liftIO $ assertEqual "INSERT ON CONFLICT does nothing (1)" (name, location) details3

  runQuery_ . sqlInsert "bank" $ do
    sqlSet "id" bankId
    sqlSet "name" ("" :: T.Text)
    sqlSet "location" ("" :: T.Text)
    sqlOnConflictOnColumnsDoNothing ["id"]
  runQuery_ . sqlSelect "bank" $ do
    sqlResult "name"
    sqlResult "location"
    sqlWhereEq "id" bankId
  details4 <- fetchOne id
  liftIO $ assertEqual "INSERT ON CONFLICT does nothing (2)" (name, location) details4

  -- If NO CONFLICT is not specified, make sure we throw an exception.
  eres :: Either DBException () <- E.try . withSavepoint "testDBSchema" $ do
    runQuery_ . sqlInsert "bank " $ do
      sqlSet "id" bankId
      sqlSet "name" name
      sqlSet "location" location
  liftIO $ assertBool "If ON CONFLICT is not specified an exception is thrown" (isLeft eres)

  -- Populate the 'bad_guy' table.
  runQuery_ . sqlInsert "bad_guy" $ do
    sqlSetList "firstname" ["Neil" :: T.Text, "Lee", "Freddie", "Frankie"
                           ,"James", "Roy"]
    sqlSetList "lastname" ["Hetzel"::T.Text, "Murray", "Foreman", "Fraser"
                          ,"Crosbie", "Shaw"]
    sqlResult "id"
  (badGuyIds :: [UUID]) <- fetchMany runIdentity
  liftIO $ assertEqual "INSERT into 'bad_guy' table" 6 (length badGuyIds)

  -- Populate the 'robbery' table.
  runQuery_ . sqlInsert "robbery" $ do
    sqlSetList "bank_id" [bankIds !! idx | idx <- [0,3]]
    sqlResult "id"
  (robberyIds :: [UUID]) <- fetchMany runIdentity
  liftIO $ assertEqual "INSERT into 'robbery' table" 2 (length robberyIds)

  -- Populate the 'participated_in_robbery' table.
  runQuery_ . sqlInsert "participated_in_robbery" $ do
    sqlSetList "bad_guy_id" [badGuyIds  !! idx | idx <- [0,2]]
    sqlSet "robbery_id" (robberyIds !! 0)
    sqlResult "bad_guy_id"
  (participatorIds :: [UUID]) <- fetchMany runIdentity
  liftIO $ assertEqual "INSERT into 'participated_in_robbery' table" 2
    (length participatorIds)

  runQuery_ . sqlInsert "participated_in_robbery" $ do
    sqlSetList "bad_guy_id" [badGuyIds  !! idx | idx <- [3,4]]
    sqlSet "robbery_id" (robberyIds !! 1)
    sqlResult "bad_guy_id"
  (participatorIds' :: [UUID]) <- fetchMany runIdentity
  liftIO $ assertEqual "INSERT into 'participated_in_robbery' table" 2
    (length participatorIds')

  -- Populate the 'witness' table.
  runQuery_ . sqlInsert "witness" $ do
    sqlSetList "firstname" ["Meredith" :: T.Text, "Charlie", "Peter", "Emun"
                           ,"Benedict"]
    sqlSetList "lastname" ["Vickers"::T.Text, "Holloway", "Weyland", "Eliott"
                          ,"Wong"]
    sqlResult "id"
  (witnessIds :: [UUID]) <- fetchMany runIdentity
  liftIO $ assertEqual "INSERT into 'witness' table" 5 (length witnessIds)

  -- Populate the 'witnessed_robbery' table.
  runQuery_ . sqlInsert "witnessed_robbery" $ do
    sqlSetList "witness_id" [witnessIds  !! idx | idx <- [0,1]]
    sqlSet "robbery_id" (robberyIds !! 0)
    sqlResult "witness_id"
  (robberyWitnessIds :: [UUID]) <- fetchMany runIdentity
  liftIO $ assertEqual "INSERT into 'witnessed_robbery' table" 2
    (length robberyWitnessIds)

  runQuery_ . sqlInsert "witnessed_robbery" $ do
    sqlSetList "witness_id" [witnessIds  !! idx | idx <- [2,3,4]]
    sqlSet "robbery_id" (robberyIds !! 1)
    sqlResult "witness_id"
  (robberyWitnessIds' :: [UUID]) <- fetchMany runIdentity
  liftIO $ assertEqual "INSERT into 'witnessed_robbery' table" 3
    (length robberyWitnessIds')

  return (badGuyIds, robberyIds)

migrateDBToSchema2 :: (String -> TestM ()) -> TestM ()
migrateDBToSchema2 step = do
  let extensions    = ["pgcrypto"]
      composites    = []
      domains       = []
  step "Migrating the database (schema version 1 -> schema version 2)..."
  migrateDatabase defaultExtrasOptions extensions composites domains
    schema2Tables schema2Migrations
  checkDatabase defaultExtrasOptions composites domains schema2Tables

-- | Hacky version of 'migrateDBToSchema2' used by 'migrationTest3'.
migrateDBToSchema2Hacky :: (String -> TestM ()) -> TestM ()
migrateDBToSchema2Hacky step = do
  let extensions    = ["pgcrypto"]
      composites    = []
      domains       = []
  step "Hackily migrating the database (schema version 1 \
       \-> schema version 2)..."
  migrateDatabase defaultExtrasOptions extensions composites domains
    schema2Tables schema2Migrations'
  checkDatabase defaultExtrasOptions composites domains schema2Tables
    where
      schema2Migrations' = createTableMigration tableFlash : schema2Migrations

testDBSchema2 :: (String -> TestM ()) -> [UUID] -> [UUID] -> TestM ()
testDBSchema2 step badGuyIds robberyIds = do
  step "Running test queries (schema version 2)..."

  -- Check that table 'witness' doesn't exist.
  runSQL_ $ "SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public'"
    <> " AND tablename = 'witness')";
  (witnessExists :: Bool) <- fetchOne runIdentity
  liftIO $ assertEqual "Table 'witness' doesn't exist" False witnessExists

  -- Check that table 'witnessed_robbery' doesn't exist.
  runSQL_ $ "SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public'"
    <> " AND tablename = 'witnessed_robbery')";
  (witnessedRobberyExists :: Bool) <- fetchOne runIdentity
  liftIO $ assertEqual "Table 'witnessed_robbery' doesn't exist" False
    witnessedRobberyExists

  -- Populate table 'under_arrest'.
  runQuery_ . sqlInsert "under_arrest" $ do
    sqlSetList "bad_guy_id" [badGuyIds  !! idx | idx <- [0,2]]
    sqlSet "robbery_id" (robberyIds !! 0)
    sqlResult "bad_guy_id"
  (arrestedIds :: [UUID]) <- fetchMany runIdentity
  liftIO $ assertEqual "INSERT into 'under_arrest' table" 2
    (length arrestedIds)

  runQuery_ . sqlInsert "under_arrest" $ do
    sqlSetList "bad_guy_id" [badGuyIds  !! idx | idx <- [3,4]]
    sqlSet "robbery_id" (robberyIds !! 1)
    sqlResult "bad_guy_id"
  (arrestedIds' :: [UUID]) <- fetchMany runIdentity
  liftIO $ assertEqual "INSERT into 'under_arrest' table" 2
    (length arrestedIds')

  return ()

migrateDBToSchema3 :: (String -> TestM ()) -> TestM ()
migrateDBToSchema3 step = do
  let extensions    = ["pgcrypto"]
      composites    = []
      domains       = []
  step "Migrating the database (schema version 2 -> schema version 3)..."
  migrateDatabase defaultExtrasOptions extensions composites domains
    schema3Tables schema3Migrations
  checkDatabase defaultExtrasOptions composites domains schema3Tables

testDBSchema3 :: (String -> TestM ()) -> [UUID] -> [UUID] -> TestM ()
testDBSchema3 step badGuyIds robberyIds = do
  step "Running test queries (schema version 3)..."

  -- Check that table 'under_arrest' doesn't exist.
  runSQL_ $ "SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public'"
    <> " AND tablename = 'under_arrest')";
  (underArrestExists :: Bool) <- fetchOne runIdentity
  liftIO $ assertEqual "Table 'under_arrest' doesn't exist" False
    underArrestExists

  -- Check that the table 'prison_sentence' exists.
  runSQL_ $ "SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public'"
    <> " AND tablename = 'prison_sentence')";
  (prisonSentenceExists :: Bool) <- fetchOne runIdentity
  liftIO $ assertEqual "Table 'prison_sentence' does exist" True
    prisonSentenceExists

  -- Populate table 'prison_sentence'.
  runQuery_ . sqlInsert "prison_sentence" $ do
    sqlSetList "bad_guy_id" [badGuyIds  !! idx | idx <- [0,2]]
    sqlSet "robbery_id" (robberyIds !! 0)
    sqlSet "sentence_length" (12::Int)
    sqlSet "prison_name" ("Long Kesh"::T.Text)
    sqlResult "bad_guy_id"
  (sentencedIds :: [UUID]) <- fetchMany runIdentity
  liftIO $ assertEqual "INSERT into 'prison_sentence' table" 2
    (length sentencedIds)

  runQuery_ . sqlInsert "prison_sentence" $ do
    sqlSetList "bad_guy_id" [badGuyIds  !! idx | idx <- [3,4]]
    sqlSet "robbery_id" (robberyIds !! 1)
    sqlSet "sentence_length" (9::Int)
    sqlSet "prison_name" ("Wormwood Scrubs"::T.Text)
    sqlResult "bad_guy_id"
  (sentencedIds' :: [UUID]) <- fetchMany runIdentity
  liftIO $ assertEqual "INSERT into 'prison_sentence' table" 2
    (length sentencedIds')

  return ()

migrateDBToSchema4 :: (String -> TestM ()) -> TestM ()
migrateDBToSchema4 step = do
  let extensions    = ["pgcrypto"]
      composites    = []
      domains       = []
  step "Migrating the database (schema version 3 -> schema version 4)..."
  migrateDatabase defaultExtrasOptions extensions composites domains
    schema4Tables schema4Migrations
  checkDatabase defaultExtrasOptions composites domains schema4Tables

testDBSchema4 :: (String -> TestM ()) -> TestM ()
testDBSchema4 step = do
  step "Running test queries (schema version 4)..."

  -- Check that the 'bank' table has a 'cash' column.
  runSQL_ $ "SELECT EXISTS (SELECT 1 FROM information_schema.columns"
    <> " WHERE table_schema = 'public'"
    <> " AND table_name = 'bank'"
    <> " AND column_name = 'cash')";
  (colCashExists :: Bool) <- fetchOne runIdentity
  liftIO $ assertEqual "Column 'cash' in the table 'bank' does exist" True
    colCashExists

  return ()

migrateDBToSchema5 :: (String -> TestM ()) -> TestM ()
migrateDBToSchema5 step = do
  let extensions    = ["pgcrypto"]
      composites    = []
      domains       = []
  step "Migrating the database (schema version 4 -> schema version 5)..."
  migrateDatabase defaultExtrasOptions extensions composites domains
    schema5Tables schema5Migrations
  checkDatabase defaultExtrasOptions composites domains schema5Tables

testDBSchema5 :: (String -> TestM ()) -> TestM ()
testDBSchema5 step = do
  step "Running test queries (schema version 5)..."

  -- Check that the 'bank' table doesn't have a 'cash' column.
  runSQL_ $ "SELECT EXISTS (SELECT 1 FROM information_schema.columns"
    <> " WHERE table_schema = 'public'"
    <> " AND table_name = 'bank'"
    <> " AND column_name = 'cash')";
  (colCashExists :: Bool) <- fetchOne runIdentity
  liftIO $ assertEqual "Column 'cash' in the table 'bank' doesn't exist" False
    colCashExists

  -- Check that the 'flash' table doesn't exist.
  runSQL_ $ "SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public'"
    <> " AND tablename = 'flash')";
  (flashExists :: Bool) <- fetchOne runIdentity
  liftIO $ assertEqual "Table 'flash' doesn't exist" False flashExists

  return ()

-- | May require 'ALTER SCHEMA public OWNER TO $user' the first time
-- you run this.
freshTestDB ::  (String -> TestM ()) -> TestM ()
freshTestDB step = do
  step "Dropping the test DB schema..."
  runSQL_ "DROP SCHEMA public CASCADE"
  runSQL_ "CREATE SCHEMA public"

-- | Re-used by 'migrationTest5'.
migrationTest1Body :: (String -> TestM ()) -> TestM ()
migrationTest1Body step = do
  createTablesSchema1 step
  (badGuyIds, robberyIds) <-
    testDBSchema1     step

  migrateDBToSchema2  step
  testDBSchema2       step badGuyIds robberyIds

  migrateDBToSchema3  step
  testDBSchema3       step badGuyIds robberyIds

  migrateDBToSchema4  step
  testDBSchema4       step

  migrateDBToSchema5  step
  testDBSchema5       step


migrationTest1 :: ConnectionSourceM (LogT IO) -> TestTree
migrationTest1 connSource =
  testCaseSteps' "Migration test 1" connSource $ \step -> do
  freshTestDB         step

  migrationTest1Body  step

  -- freshTestDB         step

-- | Test for behaviour of 'checkDatabase' and 'checkDatabaseAllowUnknownObjects'
migrationTest2 :: ConnectionSourceM (LogT IO) -> TestTree
migrationTest2 connSource =
  testCaseSteps' "Migration test 2" connSource $ \step -> do
  freshTestDB    step

  createTablesSchema1 step

  let composite = CompositeType
        { ctName = "composite"
        , ctColumns =
          [ CompositeColumn { ccName = "cint",  ccType = UuidT }
          , CompositeColumn { ccName = "ctext", ccType = TextT }
          ]
        }
      currentSchema   = schema1Tables
      differentSchema = schema5Tables
      extrasOptions   = defaultExtrasOptions { eoEnforcePKs = True }
      extrasOptionsWithUnknownObjects = extrasOptions { eoObjectsValidationMode = AllowUnknownObjects }

  runQuery_ $ sqlCreateComposite composite

  assertNoException "checkDatabase should run fine for consistent DB" $
    checkDatabase extrasOptions [composite] [] currentSchema
  assertException "checkDatabase fails if composite type definition is not provided" $
    checkDatabase extrasOptions [] [] currentSchema
  assertNoException "checkDatabaseAllowUnknownTables runs fine \
                    \for consistent DB" $
    checkDatabase extrasOptionsWithUnknownObjects [composite] [] currentSchema
  assertNoException "checkDatabaseAllowUnknownTables runs fine \
                    \for consistent DB with unknown composite type in the database" $
    checkDatabase extrasOptionsWithUnknownObjects [] [] currentSchema
  assertException "checkDatabase should throw exception for wrong schema" $
    checkDatabase extrasOptions [] [] differentSchema
  assertException ("checkDatabaseAllowUnknownObjects \
                   \should throw exception for wrong scheme") $
    checkDatabase extrasOptionsWithUnknownObjects [] [] differentSchema

  runSQL_ "INSERT INTO table_versions (name, version) \
          \VALUES ('unknown_table', 0)"
  assertException "checkDatabase throw when extra entry in 'table_versions'" $
    checkDatabase extrasOptions [] [] currentSchema
  assertNoException ("checkDatabaseAllowUnknownObjects \
                     \accepts extra entry in 'table_versions'") $
    checkDatabase extrasOptionsWithUnknownObjects [] [] currentSchema
  runSQL_ "DELETE FROM table_versions where name='unknown_table'"

  runSQL_ "CREATE TABLE unknown_table (title text)"
  assertException "checkDatabase should throw with unknown table" $
    checkDatabase extrasOptions [] [] currentSchema
  assertNoException "checkDatabaseAllowUnknownObjects accepts unknown table" $
    checkDatabase extrasOptionsWithUnknownObjects [] [] currentSchema

  runSQL_ "INSERT INTO table_versions (name, version) \
          \VALUES ('unknown_table', 0)"
  assertException "checkDatabase should throw with unknown table" $
    checkDatabase extrasOptions [] [] currentSchema
  assertNoException ("checkDatabaseAllowUnknownObjects \
                     \accepts unknown tables with version") $
    checkDatabase extrasOptionsWithUnknownObjects [] [] currentSchema

  freshTestDB    step

  let schema1TablesWithMissingPK     = schema6Tables
      schema1MigrationsWithMissingPK = schema6Migrations
      withMissingPKSchema            = schema1TablesWithMissingPK
      optionsNoPKCheck               = defaultExtrasOptions
                                       { eoEnforcePKs = False }
      optionsWithPKCheck             = defaultExtrasOptions
                                       { eoEnforcePKs = True }

  step "Recreating the database (schema version 1, one table is missing PK)..."

  migrateDatabase optionsNoPKCheck ["pgcrypto"] [] []
    schema1TablesWithMissingPK [schema1MigrationsWithMissingPK]
  checkDatabase optionsNoPKCheck [] [] withMissingPKSchema

  assertException
    "checkDatabase should throw when PK missing from table \
    \'participated_in_robbery' and check is enabled" $
    checkDatabase optionsWithPKCheck [] [] withMissingPKSchema
  assertNoException
    "checkDatabase should not throw when PK missing from table \
    \'participated_in_robbery' and check is disabled" $
    checkDatabase optionsNoPKCheck [] [] withMissingPKSchema

  freshTestDB step

migrationTest3 :: ConnectionSourceM (LogT IO) -> TestTree
migrationTest3 connSource =
  testCaseSteps' "Migration test 3" connSource $ \step -> do
  freshTestDB         step

  createTablesSchema1 step
  (badGuyIds, robberyIds) <-
    testDBSchema1     step

  migrateDBToSchema2  step
  testDBSchema2       step badGuyIds robberyIds

  assertException ( "Trying to run the same migration twice should fail, \
                     \when starting with a createTable migration" ) $
    migrateDBToSchema2Hacky  step

  freshTestDB         step

-- | Test that running the same migrations twice doesn't result in
-- unexpected errors.
migrationTest4 :: ConnectionSourceM (LogT IO) -> TestTree
migrationTest4 connSource =
  testCaseSteps' "Migration test 4" connSource $ \step -> do
  freshTestDB         step

  migrationTest1Body  step

  -- Here we run step 5 for the second time. This should be a no-op.
  migrateDBToSchema5  step
  testDBSchema5       step

  freshTestDB         step

eitherExc :: MonadBaseControl IO m =>
             (SomeException -> m ()) -> (a -> m ()) -> m a -> m ()
eitherExc left right c = (E.try c) >>= either left right

assertNoException :: String -> TestM () -> TestM ()
assertNoException t c = eitherExc
  (const $ liftIO $ assertFailure ("Exception thrown for: " ++ t))
  (const $ return ()) c

assertException :: String -> TestM () -> TestM ()
assertException   t c = eitherExc
  (const $ return ())
  (const $ liftIO $ assertFailure ("No exception thrown for: " ++ t)) c

-- | A variant of testCaseSteps that works in TestM monad.
testCaseSteps' :: TestName -> ConnectionSourceM (LogT IO)
               -> ((String -> TestM ()) -> TestM ())
               -> TestTree
testCaseSteps' testName connSource f =
  testCaseSteps testName $ \step' -> do
  let step s = liftIO $ step' s
  withSimpleStdOutLogger $ \logger ->
    runLogT "hpqtypes-extras-test" logger defaultLogLevel $
    runDBT connSource defaultTransactionSettings $
    f step

main :: IO ()
main = do
  defaultMainWithIngredients ings $
    askOption $ \(ConnectionString connectionString) ->
    let connSettings = defaultConnectionSettings
                       { csConnInfo = T.pack connectionString }
        ConnectionSource connSource = simpleSource connSettings
    in
    testGroup "DB tests" [ migrationTest1 connSource
                         , migrationTest2 connSource
                         , migrationTest3 connSource
                         , migrationTest4 connSource
                         ]
  where
    ings =
      includingOptions [Option (Proxy :: Proxy ConnectionString)]
      : defaultIngredients
