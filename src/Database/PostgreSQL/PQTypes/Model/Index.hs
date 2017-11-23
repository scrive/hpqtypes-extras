module Database.PostgreSQL.PQTypes.Model.Index (
    TableIndex(..)
  , IndexMethod(..)
  , tblIndex
  , indexOnColumn
  , indexOnColumns
  , indexOnColumnWithMethod
  , indexOnColumnsWithMethod
  , uniqueIndexOnColumn
  , uniqueIndexOnColumnWithCondition
  , uniqueIndexOnColumns
  , indexName
  , sqlCreateIndex
  , sqlDropIndex
  ) where

import Crypto.Hash.RIPEMD160
import Data.ByteString.Base16
import Data.Char
import Data.Monoid
import Data.Monoid.Utils
import Database.PostgreSQL.PQTypes
import Prelude
import qualified Data.ByteString.Char8 as BS
import qualified Data.Text as T
import qualified Data.Text.Encoding as T

data TableIndex = TableIndex {
  idxColumns :: [RawSQL ()]
, idxMethod  :: IndexMethod
, idxUnique  :: Bool
, idxWhere   :: Maybe (RawSQL ())
} deriving (Eq, Ord, Show)

data IndexMethod =
    BTree
  | GIN
  deriving (Eq, Ord)

instance Show IndexMethod where
    show BTree = "btree"
    show GIN   = "gin"

instance Read IndexMethod where
    readsPrec _ "btree" = [(BTree,"")]
    readsPrec _ "gin"   = [(GIN,"")]
    readsPrec _ _       = []

tblIndex :: TableIndex
tblIndex = TableIndex {
  idxColumns = []
, idxMethod = BTree
, idxUnique = False
, idxWhere = Nothing
}

indexOnColumn :: RawSQL () -> TableIndex
indexOnColumn column = tblIndex { idxColumns = [column] }

-- | Create an index on the given column with the specified method.  No checks
-- are made that the method is appropriate for the type of the column.
indexOnColumnWithMethod :: RawSQL () -> IndexMethod -> TableIndex
indexOnColumnWithMethod column method =
    tblIndex { idxColumns = [column]
             , idxMethod = method }

indexOnColumns :: [RawSQL ()] -> TableIndex
indexOnColumns columns = tblIndex { idxColumns = columns }

-- | Create an index on the given columns with the specified method.  No checks
-- are made that the method is appropriate for the type of the column;
-- cf. [the PostgreSQL manual](https://www.postgresql.org/docs/current/static/indexes-multicolumn.html).
indexOnColumnsWithMethod :: [RawSQL ()] -> IndexMethod -> TableIndex
indexOnColumnsWithMethod columns method =
    tblIndex { idxColumns = columns
             , idxMethod = method }

uniqueIndexOnColumn :: RawSQL () -> TableIndex
uniqueIndexOnColumn column = TableIndex {
  idxColumns = [column]
, idxMethod = BTree
, idxUnique = True
, idxWhere = Nothing
}

uniqueIndexOnColumns :: [RawSQL ()] -> TableIndex
uniqueIndexOnColumns columns = TableIndex {
  idxColumns = columns
, idxMethod = BTree
, idxUnique = True
, idxWhere = Nothing
}

uniqueIndexOnColumnWithCondition :: RawSQL () -> RawSQL () -> TableIndex
uniqueIndexOnColumnWithCondition column whereC = TableIndex {
  idxColumns = [column]
, idxMethod = BTree
, idxUnique = True
, idxWhere = Just whereC
}

indexName :: RawSQL () -> TableIndex -> RawSQL ()
indexName tname TableIndex{..} = flip rawSQL () $ T.take 63 . unRawSQL $ mconcat [
    if idxUnique then "unique_idx__" else "idx__"
  , tname
  , "__"
  , mintercalate "__" $ map (asText sanitize) idxColumns
  , maybe "" (("__" <>) . hashWhere) idxWhere
  ]
  where
    asText f = flip rawSQL () . f . unRawSQL
    -- See http://www.postgresql.org/docs/9.4/static/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS.
    -- Remove all unallowed characters and replace them by at most one adjacent dollar sign.
    sanitize = T.pack . foldr go [] . T.unpack
      where
        go c acc = if isAlphaNum c || c == '_'
          then c : acc
          else case acc of
            ('$':_) ->       acc
            _       -> '$' : acc
    -- hash WHERE clause and add it to index name so that indexes
    -- with the same columns, but different constraints can coexist
    hashWhere = asText $ T.decodeUtf8 . encode . BS.take 10 . hash . T.encodeUtf8

sqlCreateIndex :: RawSQL () -> TableIndex -> RawSQL ()
sqlCreateIndex tname idx@TableIndex{..} = mconcat [
    "CREATE "
  , if idxUnique then "UNIQUE " else ""
  , "INDEX" <+> indexName tname idx <+> "ON" <+> tname <+> ""
  , "USING" <+> (rawSQL (T.pack . show $ idxMethod) ()) <+> "("
  , mintercalate ", " idxColumns
  , ")"
  , maybe "" (" WHERE" <+>) idxWhere
  ]

sqlDropIndex :: RawSQL () -> TableIndex -> RawSQL ()
sqlDropIndex tname idx = "DROP INDEX" <+> indexName tname idx
