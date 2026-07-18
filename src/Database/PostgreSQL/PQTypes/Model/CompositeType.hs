module Database.PostgreSQL.PQTypes.Model.CompositeType
  ( CompositeType (..)
  , CompositeColumn (..)
  , compositeTypePqFormat
  , sqlCreateComposite
  , sqlDropComposite
  , getDBCompositeTypes
  ) where

import Data.ByteString qualified as BS
import Data.Foldable (toList)
import Data.Monoid.Utils
import Data.Text.Encoding qualified as T
import Database.PostgreSQL.PQTypes

import Database.PostgreSQL.PQTypes.Model.ColumnType
import Database.PostgreSQL.PQTypes.SQL.Builder

data CompositeType = CompositeType
  { ctName :: !(RawSQL ())
  , ctColumns :: ![CompositeColumn]
  }
  deriving (Eq, Ord, Show)

data CompositeColumn = CompositeColumn
  { ccName :: !(RawSQL ())
  , ccType :: ColumnType
  }
  deriving (Eq, Ord, Show)

-- | Convenience function for converting CompositeType definition to
-- corresponding 'pqFormat' definition.
compositeTypePqFormat :: CompositeType -> BS.ByteString
compositeTypePqFormat ct = "%" `BS.append` T.encodeUtf8 (unRawSQL $ ctName ct)

-- | Make SQL query that creates a composite type.
sqlCreateComposite :: CompositeType -> RawSQL ()
sqlCreateComposite CompositeType {..} =
  smconcat
    [ "CREATE TYPE"
    , ctName
    , "AS ("
    , mintercalate ", " $ map columnToSQL ctColumns
    , ")"
    ]
  where
    columnToSQL CompositeColumn {..} = ccName <+> columnTypeToSQL ccType

-- | Make SQL query that drops a composite type.
sqlDropComposite :: RawSQL () -> RawSQL ()
sqlDropComposite = ("DROP TYPE" <+>)

----------------------------------------

-- | Get composite types defined in the database.
getDBCompositeTypes :: MonadDB m => m [CompositeType]
getDBCompositeTypes = do
  -- Each composite type's columns are aggregated into an array of (name, type)
  -- records, so a single query is enough to fetch everything.
  runQuery_ . sqlSelect "pg_catalog.pg_class c" $ do
    sqlResult "c.relname::text"
    sqlResultArray . sqlSelect "pg_catalog.pg_attribute a" $ do
      sqlResult "a.attname::text"
      sqlResult "pg_catalog.format_type(a.atttypid, a.atttypmod)"
      sqlWhere "a.attrelid = c.oid"
      sqlOrderBy "a.attnum"
    sqlWhere "pg_catalog.pg_table_is_visible(c.oid)"
    sqlWhereEq "c.relkind" 'c'
    sqlOrderBy "c.relname"
  fetchMany $ do
    name <- fromSQL
    columns <- decodeArray . decodeComposite $ do
      cname <- fromSQL
      ctype <- fromSQL
      pure CompositeColumn {ccName = rawSQL cname (), ccType = ctype}
    pure CompositeType {ctName = rawSQL name (), ctColumns = toList columns}
