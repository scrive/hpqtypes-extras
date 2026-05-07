{-# OPTIONS_GHC -Wno-incomplete-patterns #-}

module Database.PostgreSQL.PQTypes.Model.ColumnType
  ( ColumnType (..)
  , columnTypeToSQL
  ) where

import Data.Attoparsec.Text qualified as A
import Data.Functor
import Data.Text qualified as T
import Database.PostgreSQL.PQTypes

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
  | ArrayT !ColumnType
  | CustomT !(RawSQL ())
  | NumericT !(Maybe (Int, Maybe Int))
  deriving (Ord, Show)

instance Eq ColumnType where
  BigIntT == BigIntT = True
  BigSerialT == BigSerialT = True
  BinaryT == BinaryT = True
  BoolT == BoolT = True
  DateT == DateT = True
  DoubleT == DoubleT = True
  IntegerT == IntegerT = True
  UuidT == UuidT = True
  IntervalT == IntervalT = True
  JsonT == JsonT = True
  JsonbT == JsonbT = True
  SmallIntT == SmallIntT = True
  TextT == TextT = True
  TimestampWithZoneT == TimestampWithZoneT = True
  TSVectorT == TSVectorT = True
  XmlT == XmlT = True
  ArrayT t == ArrayT t' = t == t'
  CustomT t == CustomT t' = t == t'
  NumericT (Just (precision, Nothing)) == NumericT (Just (precision', Just 0)) = precision == precision'
  NumericT (Just (precision, Just 0)) == NumericT (Just (precision', Nothing)) = precision == precision'
  NumericT mPrecisionScale == NumericT mPrecisionScale' = mPrecisionScale == mPrecisionScale'
  _ == _ = False

instance PQFormat ColumnType where
  pqFormat = pqFormat @T.Text
instance FromSQL ColumnType where
  type PQBase ColumnType = PQBase T.Text
  fromSQL mbase = parseType . T.toLower <$> fromSQL mbase

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
  tname -> case parseNumeric tname of
    Just t -> t
    Nothing
      | "[]" `T.isSuffixOf` tname -> ArrayT . parseType $ T.take (T.length tname - 2) tname
      | otherwise -> CustomT $ rawSQL tname ()

parseNumeric :: T.Text -> Maybe ColumnType
parseNumeric tname =
  let inParens p = A.string "(" *> p <* A.string ")"
      comma = A.string "," $> ()
      scale = do
        s <- A.signed A.decimal
        pure $
          if s == 0
            then
              Nothing
            else
              Just s
      precisionAndScale = Just <$> inParens ((,) <$> (A.decimal <* comma) <*> scale)
      precisionOnly = Just . (,Nothing) <$> inParens A.decimal
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
columnTypeToSQL (ArrayT t) = columnTypeToSQL t <> "[]"
columnTypeToSQL (CustomT tname) = tname
columnTypeToSQL (NumericT Nothing) = rawSQL "NUMERIC" ()
columnTypeToSQL (NumericT (Just (precision, Nothing))) = rawSQL ("NUMERIC(" <> T.pack (show precision) <> ")") ()
columnTypeToSQL (NumericT (Just (precision, Just 0))) = rawSQL ("NUMERIC(" <> T.pack (show precision) <> ")") ()
columnTypeToSQL (NumericT (Just (precision, Just scale))) = rawSQL ("NUMERIC(" <> T.pack (show precision) <> "," <> T.pack (show scale) <> ")") ()
