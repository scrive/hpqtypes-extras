module Database.PostgreSQL.PQTypes.Model.ColumnType (
    ColumnType(..)
  , columnTypeToSQL
  ) where

import Control.Applicative ((<$>))
import Data.Monoid
import Database.PostgreSQL.PQTypes
import Prelude
import qualified Data.Text as T

data ColumnType
  = BigIntT
  | BigSerialT
  | BinaryT
  | BoolT
  | DateT
  | DoubleT
  | IntegerT
  | IntervalT
  | JsonT
  | JsonbT
  | SmallIntT
  | TextT
  | TimestampWithZoneT
  | XmlT
  | ArrayT !ColumnType
  | CustomT !(RawSQL ())
    deriving (Eq, Ord, Show)

instance PQFormat ColumnType where
  pqFormat = const $ pqFormat (undefined::T.Text)
instance FromSQL ColumnType where
  type PQBase ColumnType = PQBase T.Text
  fromSQL mbase = parseType . T.toLower <$> fromSQL mbase
    where
      parseType :: T.Text -> ColumnType
      parseType = \case
        "bigint" -> BigIntT
        "bytea" -> BinaryT
        "boolean" -> BoolT
        "date" -> DateT
        "double precision" -> DoubleT
        "integer" -> IntegerT
        "interval" -> IntervalT
        "json" -> JsonT
        "jsonb" -> JsonbT
        "smallint" -> SmallIntT
        "text" -> TextT
        "timestamp with time zone" -> TimestampWithZoneT
        "xml" -> XmlT
        tname
          | "[]" `T.isSuffixOf` tname -> ArrayT . parseType $ T.take (T.length tname - 2) tname
          | otherwise -> CustomT $ rawSQL tname ()

columnTypeToSQL :: ColumnType -> RawSQL ()
columnTypeToSQL BigIntT            = "BIGINT"
columnTypeToSQL BigSerialT         = "BIGSERIAL"
columnTypeToSQL BinaryT            = "BYTEA"
columnTypeToSQL BoolT              = "BOOLEAN"
columnTypeToSQL DateT              = "DATE"
columnTypeToSQL DoubleT            = "DOUBLE PRECISION"
columnTypeToSQL IntegerT           = "INTEGER"
columnTypeToSQL IntervalT          = "INTERVAL"
columnTypeToSQL JsonT              = "JSON"
columnTypeToSQL JsonbT             = "JSONB"
columnTypeToSQL SmallIntT          = "SMALLINT"
columnTypeToSQL TextT              = "TEXT"
columnTypeToSQL TimestampWithZoneT = "TIMESTAMPTZ"
columnTypeToSQL XmlT               = "XML"
columnTypeToSQL (ArrayT t)         = columnTypeToSQL t <> "[]"
columnTypeToSQL (CustomT tname)    = tname
