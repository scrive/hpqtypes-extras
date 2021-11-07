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
  , sqlCreateIndexMaybeDowntime
  , sqlCreateIndexConcurrently
  , sqlDropIndexMaybeDowntime
  , sqlDropIndexConcurrently
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
, idxValid   :: Bool -- ^ If creation of index with CONCURRENTLY fails, index
                     -- will be marked as invalid. Set it to 'False' if such
                     -- situation is expected.
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
    readsPrec _ (map toLower -> "btree") = [(BTree,"")]
    readsPrec _ (map toLower -> "gin")   = [(GIN,"")]
    readsPrec _ _       = []

tblIndex :: TableIndex
tblIndex = TableIndex {
  idxColumns = []
, idxMethod = BTree
, idxUnique = False
, idxValid = True
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
, idxValid = True
, idxWhere = Nothing
}

uniqueIndexOnColumns :: [RawSQL ()] -> TableIndex
uniqueIndexOnColumns columns = TableIndex {
  idxColumns = columns
, idxMethod = BTree
, idxUnique = True
, idxValid = True
, idxWhere = Nothing
}

uniqueIndexOnColumnWithCondition :: RawSQL () -> RawSQL () -> TableIndex
uniqueIndexOnColumnWithCondition column whereC = TableIndex {
  idxColumns = [column]
, idxMethod = BTree
, idxUnique = True
, idxValid = True
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

-- | Create an index. Warning: if the affected table is large, this will prevent
-- the table from being modified during the creation. If this is not acceptable,
-- use 'CreateIndexConcurrentlyMigration'. See
-- https://www.postgresql.org/docs/current/sql-createindex.html for more
-- information.
sqlCreateIndexMaybeDowntime :: RawSQL () -> TableIndex -> RawSQL ()
sqlCreateIndexMaybeDowntime = sqlCreateIndex_ False

-- | Create index concurrently.
sqlCreateIndexConcurrently :: RawSQL () -> TableIndex -> RawSQL ()
sqlCreateIndexConcurrently = sqlCreateIndex_ True

sqlCreateIndex_ :: Bool -> RawSQL () -> TableIndex -> RawSQL ()
sqlCreateIndex_ concurrently tname idx@TableIndex{..} = mconcat [
    "CREATE"
  , if idxUnique then " UNIQUE" else ""
  , " INDEX "
  , if concurrently then "CONCURRENTLY " else ""
  , indexName tname idx
  , " ON" <+> tname
  , " USING" <+> (rawSQL (T.pack . show $ idxMethod) ()) <+> "("
  , mintercalate ", " idxColumns
  , ")"
  , maybe "" (" WHERE" <+>) idxWhere
  ]

-- | Drop an index. Warning: if you don't want to lock out concurrent operations
-- on the index's table, use 'DropIndexConcurrentlyMigration'. See
-- https://www.postgresql.org/docs/current/sql-dropindex.html for more
-- information.
sqlDropIndexMaybeDowntime :: RawSQL () -> TableIndex -> RawSQL ()
sqlDropIndexMaybeDowntime tname idx = "DROP INDEX" <+> indexName tname idx

sqlDropIndexConcurrently :: RawSQL () -> TableIndex -> RawSQL ()
sqlDropIndexConcurrently tname idx = "DROP INDEX CONCURRENTLY" <+> indexName tname idx
