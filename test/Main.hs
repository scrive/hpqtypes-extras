module Main
  where

import Control.Monad.IO.Class
import Data.Int
import qualified Data.Text as T
import Data.Typeable

import Database.PostgreSQL.PQTypes
import Database.PostgreSQL.PQTypes.Checks
import Database.PostgreSQL.PQTypes.Model.ColumnType
import Database.PostgreSQL.PQTypes.Model.ForeignKey
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
  defaultValue = ConnectionString "postgresql://postgres@localhost/travis_ci_test"
  parseValue = Just . ConnectionString
  optionName = return "connection-string"
  optionHelp = return "Postgres connection string"

-- Simple example schemata inspired by the one in
-- <http://www.databaseanswers.org/data_models/bank_robberies/index.htm>
--
-- Schema 1: Bank robberies, tables:
--           bank, bad_guy, robbery, participated_in_robbery, witness,
--           witnessed_robbery
-- Schema 2: Witnesses go into witness protection program,
--           (some) bad guys get arrested:
--           drop tables witness and witnessed_by,
--           add table under_arrest
-- Schema 3: Bad guys get their prison sentences:
--           drop table under_arrest
--           add table prison_sentence
-- Cleanup:  drop all schema 3 sentences

tableBankSchema1 :: Table
tableBankSchema1 =
  tblTable
  { tblName = "bank"
  , tblVersion = 1
  , tblColumns =
    [ tblColumn { colName = "id",       colType = BigSerialT
                , colNullable = False }
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

tableBadGuySchema1 :: Table
tableBadGuySchema1 =
  tblTable
  { tblName = "bad_guy"
  , tblVersion = 1
  , tblColumns =
    [ tblColumn { colName = "id",        colType = BigSerialT
                , colNullable = False }
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

tableRobberySchema1 :: Table
tableRobberySchema1 =
  tblTable
  { tblName = "robbery"
  , tblVersion = 1
  , tblColumns =
    [ tblColumn { colName = "id",      colType = BigSerialT, colNullable = False }
    , tblColumn { colName = "bank_id", colType = BigIntT,    colNullable = False }
    , tblColumn { colName = "date",    colType = DateT
                , colNullable = False, colDefault = Just "now()" }
    ]
  , tblPrimaryKey  = pkOnColumn "id"
  , tblForeignKeys = [fkOnColumn "bank_id" "bank" "id"] }

tableRobberySchema2 :: Table
tableRobberySchema2 = tableRobberySchema1

tableRobberySchema3 :: Table
tableRobberySchema3 = tableRobberySchema2

tableParticipatedInRobberySchema1 :: Table
tableParticipatedInRobberySchema1 =
  tblTable
  { tblName = "participated_in_robbery"
  , tblVersion = 1
  , tblColumns =
    [ tblColumn { colName = "bad_guy_id", colType = BigIntT, colNullable = False }
    , tblColumn { colName = "robbery_id", colType = BigIntT, colNullable = False }
    ]
  , tblPrimaryKey  = pkOnColumns ["bad_guy_id", "robbery_id"]
  , tblForeignKeys = [fkOnColumn  "bad_guy_id" "bad_guy" "id"
                     ,fkOnColumn  "robbery_id" "robbery" "id"] }

tableParticipatedInRobberySchema2 :: Table
tableParticipatedInRobberySchema2 = tableParticipatedInRobberySchema1

tableParticipatedInRobberySchema3 :: Table
tableParticipatedInRobberySchema3 = tableParticipatedInRobberySchema2

tableWitnessName :: RawSQL ()
tableWitnessName = "witness"

tableWitnessSchema1 :: Table
tableWitnessSchema1 =
  tblTable
  { tblName = tableWitnessName
  , tblVersion = 1
  , tblColumns =
    [ tblColumn { colName = "id",        colType = BigSerialT
                , colNullable = False }
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
    [ tblColumn { colName = "witness_id", colType = BigIntT, colNullable = False }
    , tblColumn { colName = "robbery_id", colType = BigIntT, colNullable = False }
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
    [ tblColumn { colName = "bad_guy_id", colType = BigIntT, colNullable = False }
    , tblColumn { colName = "robbery_id", colType = BigIntT, colNullable = False }
    , tblColumn { colName = "court_date", colType = DateT,   colNullable = False }
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
    [ tblColumn { colName = "bad_guy_id", colType = BigIntT, colNullable = False }
    , tblColumn { colName = "robbery_id", colType = BigIntT, colNullable = False }
    , tblColumn { colName = "sentence_start"
                , colType = DateT
                , colNullable = False }
    , tblColumn { colName = "sentence_length"
                , colType = IntegerT
                , colNullable = False }
    , tblColumn { colName = "prison_name"
                , colType = TextT
                , colNullable = False }
    ]
  , tblPrimaryKey  = pkOnColumns ["bad_guy_id", "robbery_id"]
  , tblForeignKeys = [fkOnColumn  "bad_guy_id" "bad_guy" "id"
                     ,fkOnColumn  "robbery_id" "robbery" "id"] }


createTableMigration :: (MonadDB m) => Table -> Migration m
createTableMigration tbl = Migration
  { mgrTable = tbl
  , mgrFrom  = 0
  , mgrType  = StandardMigration $ do
      createTable True tbl
  }

dropTableMigration :: (MonadDB m) => Table -> Migration m
dropTableMigration tbl = Migration
  { mgrTable = tbl
  , mgrFrom  = tblVersion tbl
  , mgrType  = DropTableMigration DropTableCascade
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

type TestM a = DBT (LogT IO) a

createTablesSchema1 :: (String -> TestM ()) -> TestM ()
createTablesSchema1 step = do
  step "Creating the database (schema version 1)..."
  migrateDatabase {- options -} [] {- extensions -} [] {- domains -} []
    schema1Tables schema1Migrations
  checkDatabase {- domains -} [] schema1Tables

testDBSchema1 :: (String -> TestM ()) -> TestM ()
testDBSchema1 step = do
  step "Running test queries (schema version 1)..."
  runQuery_ . sqlInsert "bank" $ do
    sqlSetList "name" ["HSBC" :: T.Text, "Swedbank", "Nordea", "Citi"
                      ,"Wells Fargo"]
    sqlSetList "location" ["13 Foo St., Tucson, AZ, USa" :: T.Text
                          , "18 Bargatan, Stockholm, Sweden"
                          , "23 Baz Lane, Liverpool, UK"
                          , "2/3 Quux Ave., Milton Keynes, UK"
                          , "6600 Sunset Blvd., Los Angeles, CA, USA"]
    sqlResult "id"
  (_bankIds :: [Int64]) <- fetchMany runIdentity

  runQuery_ . sqlInsert "bad_guy" $ do
    sqlSetList "firstname" ["Neil" :: T.Text, "Lee", "Freddie", "Frankie"
                           ,"James", "Roy"]
    sqlSetList "lastname" ["Hetzel"::T.Text, "Murray", "Foreman", "Fraser"
                          ,"Crosbie", "Shaw"]
    sqlResult "id"
  (_badGuyIds :: [Int64]) <- fetchMany runIdentity

  return ()

migrateDBToSchema2 :: (String -> TestM ()) -> TestM ()
migrateDBToSchema2 step = do
  step "Migrating the database (schema version 1 -> schema version 2)..."
  migrateDatabase {- options -} [] {- extensions -} [] {- domains -} []
    schema2Tables schema2Migrations
  checkDatabase {- domains -} [] schema2Tables

testDBSchema2 :: (String -> TestM ()) -> TestM ()
testDBSchema2 step = do
  step "Running test queries (schema version 2)..."
  return ()

migrateDBToSchema3 :: (String -> TestM ()) -> TestM ()
migrateDBToSchema3 step = do
  step "Migrating the database (schema version 2 -> schema version 3)..."
  migrateDatabase {- options -} [] {- extensions -} [] {- domains -} []
    schema3Tables schema3Migrations
  checkDatabase {- domains -} [] schema3Tables

testDBSchema3 :: (String -> TestM ()) -> TestM ()
testDBSchema3 step = do
  step "Running test queries (schema version 3)..."
  return ()

finalDropTableMigrations :: (MonadDB m) => [Migration m]
finalDropTableMigrations =
  schema3Migrations ++
  [ dropTableMigration tbl | tbl <- reverse schema3Tables ]

dropTablesFinal :: (String -> TestM ()) -> TestM ()
dropTablesFinal step = do
  step "Dropping database tables (schema 3)..."
  migrateDatabase {- options -} [] {- extensions -} [] {- domains -} []
    {- tables -} [] finalDropTableMigrations
  checkDatabase {- domains -} [] {- tables -} []

main :: IO ()
main = do
  defaultMainWithIngredients ings $
    askOption $ \(ConnectionString connectionString) ->
    testCaseSteps "DB tests" $ \step' -> do
      let connSettings = def { csConnInfo = T.pack connectionString }
          ConnectionSource connSource = simpleSource connSettings
          step s = liftIO $ step' s
      withSimpleStdOutLogger $ \logger ->
        runLogT "hpqtypes-extras-test" logger $
        runDBT connSource {- transactionSettings -} def $
        runTests step
  where
    runTests step = do
      createTablesSchema1 step
      testDBSchema1       step

      migrateDBToSchema2  step
      testDBSchema2       step

      migrateDBToSchema3  step
      testDBSchema3       step

      dropTablesFinal     step

    ings =
      includingOptions [Option (Proxy :: Proxy ConnectionString)]
      : defaultIngredients
