{-# LANGUAGE ExistentialQuantification #-}
module DB.Model.Table (
    TableColumn(..)
  , tblColumn
  , sqlAddColumn
  , sqlAlterColumn
  , sqlDropColumn
  , Rows(..)
  , Table(..)
  , tblTable
  , sqlCreateTable
  , sqlAlterTable
  , TableInitialSetup(..)
  ) where

import Control.Monad.Catch
import Data.ByteString (ByteString)
import Data.Int
import Data.Monoid.Utils
import Database.PostgreSQL.PQTypes
import Prelude

import DB.Model.Check
import DB.Model.ColumnType
import DB.Model.ForeignKey
import DB.Model.Index
import DB.Model.PrimaryKey

data TableColumn = TableColumn {
  colName     :: RawSQL ()
, colType     :: ColumnType
, colNullable :: Bool
, colDefault  :: Maybe (RawSQL ())
} deriving Show

tblColumn :: TableColumn
tblColumn = TableColumn {
  colName = error "tblColumn: column name must be specified"
, colType = error "tblColumn: column type must be specified"
, colNullable = True
, colDefault = Nothing
}

sqlAddColumn :: TableColumn -> RawSQL ()
sqlAddColumn TableColumn{..} = smconcat [
    "ADD COLUMN"
  , colName
  , columnTypeToSQL colType
  , if colNullable then "NULL" else "NOT NULL"
  , maybe "" ("DEFAULT" <+>) colDefault
  ]

sqlAlterColumn :: RawSQL () -> RawSQL () -> RawSQL ()
sqlAlterColumn cname alter = "ALTER COLUMN" <+> cname <+> alter

sqlDropColumn :: RawSQL () -> RawSQL ()
sqlDropColumn cname = "DROP COLUMN" <+> cname

----------------------------------------

data Rows = forall row. (Show row, ToRow row) => Rows [ByteString] [row]

data Table = Table {
  tblName          :: RawSQL ()
, tblVersion       :: Int32
, tblColumns       :: [TableColumn]
, tblPrimaryKey    :: Maybe PrimaryKey
, tblChecks        :: [Check]
, tblForeignKeys   :: [ForeignKey]
, tblIndexes       :: [TableIndex]
, tblInitialSetup  :: Maybe TableInitialSetup
}

data TableInitialSetup = TableInitialSetup {
  checkInitialSetup :: forall m. (MonadDB m, MonadThrow m) => m Bool
, initialSetup      :: forall m. (MonadDB m, MonadThrow m) => m ()
}

tblTable :: Table
tblTable = Table {
  tblName = error "tblTable: table name must be specified"
, tblVersion = error "tblTable: table version must be specified"
, tblColumns = error "tblTable: table columns must be specified"
, tblPrimaryKey = Nothing
, tblChecks = []
, tblForeignKeys = []
, tblIndexes = []
, tblInitialSetup = Nothing
}

sqlCreateTable :: RawSQL () -> RawSQL ()
sqlCreateTable tname = "CREATE TABLE" <+> tname <+> "()"

sqlAlterTable :: RawSQL () -> [RawSQL ()] -> RawSQL ()
sqlAlterTable tname alter_statements = smconcat [
    "ALTER TABLE"
  , tname
  , mintercalate ", " alter_statements
  ]
