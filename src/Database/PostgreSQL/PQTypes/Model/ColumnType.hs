module Database.PostgreSQL.PQTypes.Model.ColumnType
  ( ColumnType (..)
  , columnTypeToSQL
  ) where

import Data.Attoparsec.Text qualified as A
import Data.Functor
import Data.Text qualified as T
import Database.PostgreSQL.PQTypes
import TextShow

data ColumnType
  = BigIntT
  | BigSerialT
  | BinaryT
  | BoolT
  | DateT
  | DoubleT
  | IntegerT
  | UuidT
  | IntervalT
  | JsonT
  | JsonbT
  | SmallIntT
  | TextT
  | TimestampWithZoneT
  | TSVectorT
  | XmlT
  | InetT
  | Int4RangeT
  | Int8RangeT
  | NumRangeT
  | DateRangeT
  | TSRangeT
  | TSTZRangeT
  | ArrayT !ColumnType
  | CustomT !(RawSQL ())
  | NumericT !(Maybe (Int, Int))
  deriving (Eq, Ord, Show)

instance FromSQL ColumnType where
  fromSQL = parseType . T.toLower <$> fromSQL
    where
      parseType :: T.Text -> ColumnType
      parseType = \case
        "bigint" -> BigIntT
        "bytea" -> BinaryT
        "boolean" -> BoolT
        "date" -> DateT
        "double precision" -> DoubleT
        "integer" -> IntegerT
        "uuid" -> UuidT
        "interval" -> IntervalT
        "json" -> JsonT
        "jsonb" -> JsonbT
        "smallint" -> SmallIntT
        "text" -> TextT
        "timestamp with time zone" -> TimestampWithZoneT
        "tsvector" -> TSVectorT
        "xml" -> XmlT
        "inet" -> InetT
        "int4range" -> Int4RangeT
        "int8range" -> Int8RangeT
        "numrange" -> NumRangeT
        "daterange" -> DateRangeT
        "tsrange" -> TSRangeT
        "tstzrange" -> TSTZRangeT
        tname -> case parseNumeric tname of
          Just t -> t
          Nothing
            | "[]" `T.isSuffixOf` tname -> ArrayT . parseType $ T.take (T.length tname - 2) tname
            | otherwise -> CustomT $ rawSQL tname ()
      parseNumeric :: T.Text -> Maybe ColumnType
      parseNumeric tname =
        let inParens p = A.string "(" *> p <* A.string ")"
            comma = A.string "," $> ()
            precisionAndScale = Just <$> inParens ((,) <$> (A.decimal <* comma) <*> A.signed A.decimal)
            precisionOnly = Just . (,0) <$> inParens A.decimal
            numericParser =
              NumericT
                <$> ( A.string "numeric"
                        *> A.choice
                          [ precisionAndScale
                          , precisionOnly
                          , pure Nothing
                          ]
                    )
        in either (const Nothing) Just $ A.parseOnly numericParser tname

columnTypeToSQL :: ColumnType -> RawSQL ()
columnTypeToSQL BigIntT = "BIGINT"
columnTypeToSQL BigSerialT = "BIGSERIAL"
columnTypeToSQL BinaryT = "BYTEA"
columnTypeToSQL BoolT = "BOOLEAN"
columnTypeToSQL DateT = "DATE"
columnTypeToSQL DoubleT = "DOUBLE PRECISION"
columnTypeToSQL IntegerT = "INTEGER"
columnTypeToSQL UuidT = "UUID"
columnTypeToSQL IntervalT = "INTERVAL"
columnTypeToSQL JsonT = "JSON"
columnTypeToSQL JsonbT = "JSONB"
columnTypeToSQL SmallIntT = "SMALLINT"
columnTypeToSQL TextT = "TEXT"
columnTypeToSQL TSVectorT = "TSVECTOR"
columnTypeToSQL TimestampWithZoneT = "TIMESTAMPTZ"
columnTypeToSQL XmlT = "XML"
columnTypeToSQL InetT = "INET"
columnTypeToSQL Int4RangeT = "INT4RANGE"
columnTypeToSQL Int8RangeT = "INT8RANGE"
columnTypeToSQL NumRangeT = "NUMRANGE"
columnTypeToSQL DateRangeT = "DATERANGE"
columnTypeToSQL TSRangeT = "TSRANGE"
columnTypeToSQL TSTZRangeT = "TSTZRANGE"
columnTypeToSQL (ArrayT t) = columnTypeToSQL t <> "[]"
columnTypeToSQL (CustomT tname) = tname
columnTypeToSQL (NumericT Nothing) = rawSQL "NUMERIC" ()
columnTypeToSQL (NumericT (Just (precision, scale))) = rawSQL ("NUMERIC(" <> showt precision <> "," <> showt scale <> ")") ()
