module Database.PostgreSQL.PQTypes.Model.Index
  ( TableIndex (..)
  , IndexColumn (..)
  , indexColumn
  , indexColumnWithOperatorClass
  , IndexMethod (..)
  , tblIndex
  , indexOnColumn
  , indexOnColumns
  , indexOnColumnWithMethod
  , indexOnColumnsWithMethod
  , indexColumnName
  , uniqueIndexOnColumn
  , uniqueIndexOnColumnWithCondition
  , uniqueIndexOnColumns
  , indexName
  , IncludeColumn (unIncludeColumn)
  , includeColumn
  , sqlCreateIndexMaybeDowntime
  , sqlCreateIndexConcurrently
  , sqlDropIndexMaybeDowntime
  , sqlDropIndexConcurrently
  ) where

import Crypto.Hash.RIPEMD160 qualified as RIPEMD160
import Data.ByteString.Base16 qualified as B16
import Data.ByteString.Char8 qualified as BS
import Data.Char
import Data.Function
import Data.Monoid.Utils
import Data.String
import Data.Text qualified as T
import Data.Text.Encoding qualified as T
import Database.PostgreSQL.PQTypes

data TableIndex = TableIndex
  { idxColumns :: [IndexColumn]
  , idxInclude :: [IncludeColumn]
  , idxMethod :: IndexMethod
  , idxUnique :: Bool
  , -- \^ If creation of index with CONCURRENTLY fails, index
    -- will be marked as invalid. Set it to 'False' if such
    -- situation is expected.
    idxWhere :: Maybe (RawSQL ())
  , idxNotDistinctNulls :: Bool
  }
  -- \^ Adds NULL NOT DISTINCT on the index, meaning that
  -- \^ only one NULL value will be accepted; other NULLs
  -- \^ will be perceived as a violation of the constraint.
  -- \^ NB: will only be used if idxUnique is set to True
  deriving (Eq, Ord, Show)

-- | A key column of an index, optionally with an operator class. The 'Bool'
-- records whether the identifier was surrounded by double quotes in the source.
data IndexColumn
  = IndexColumn (RawSQL ()) (Maybe (RawSQL ())) Bool
  deriving (Show)

-- If one of the two columns doesn't specify the operator class, we just ignore
-- it and still treat them as equivalent.
instance Eq IndexColumn where
  IndexColumn x Nothing _ == IndexColumn y _ _ = x == y
  IndexColumn x _ _ == IndexColumn y Nothing _ = x == y
  IndexColumn x (Just x') _ == IndexColumn y (Just y') _ = x == y && x' == y'

instance Ord IndexColumn where
  compare = compare `on` indexColumnName

instance IsString IndexColumn where
  fromString = indexColumn . fromString

-- | A column appearing in an @INCLUDE@ clause. The 'Bool' records whether the
-- identifier was surrounded by double quotes in the source.
data IncludeColumn = IncludeColumn {unIncludeColumn :: RawSQL (), _incWasQuoted :: Bool}
  deriving (Show)

instance Eq IncludeColumn where
  IncludeColumn x _ == IncludeColumn y _ = x == y

instance Ord IncludeColumn where
  compare = compare `on` unIncludeColumn

instance IsString IncludeColumn where
  fromString = includeColumn . fromString

-- | Strip a single surrounding pair of double quotes. The 'Bool' reports
-- whether stripping happened.
mkIdent :: RawSQL () -> (RawSQL (), Bool)
mkIdent r = case fmap T.unsnoc <$> T.uncons (unRawSQL r) of
  Just ('"', Just (body, '"')) -> (rawSQL body (), True)
  _ -> (r, False)

-- | Wrap an identifier in double quotes for DDL emission.
quoteIdent :: RawSQL () -> RawSQL ()
quoteIdent r = "\"" <> r <> "\""

-- | Apply 'quoteIdent' iff the bit is set.
quoteWhen :: Bool -> RawSQL () -> RawSQL ()
quoteWhen True = quoteIdent
quoteWhen False = id

-- | Lift a 'Text' transformation onto the body of a 'RawSQL' ().
asText :: (T.Text -> T.Text) -> RawSQL () -> RawSQL ()
asText f = (`rawSQL` ()) . f . unRawSQL

indexColumn :: RawSQL () -> IndexColumn
indexColumn col = let (c, q) = mkIdent col in IndexColumn c Nothing q

indexColumnWithOperatorClass :: RawSQL () -> RawSQL () -> IndexColumn
indexColumnWithOperatorClass col opclass =
  let (c, q) = mkIdent col in IndexColumn c (Just opclass) q

indexColumnName :: IndexColumn -> RawSQL ()
indexColumnName (IndexColumn col _ _) = col

includeColumn :: RawSQL () -> IncludeColumn
includeColumn col = let (c, q) = mkIdent col in IncludeColumn c q

data IndexMethod
  = BTree
  | GIN
  deriving (Eq, Ord, Show)

tblIndex :: TableIndex
tblIndex =
  TableIndex
    { idxColumns = []
    , idxInclude = []
    , idxMethod = BTree
    , idxUnique = False
    , idxWhere = Nothing
    , idxNotDistinctNulls = False
    }

indexOnColumn :: IndexColumn -> TableIndex
indexOnColumn column = tblIndex {idxColumns = [column]}

-- | Create an index on the given column with the specified method.  No checks
-- are made that the method is appropriate for the type of the column.
indexOnColumnWithMethod :: IndexColumn -> IndexMethod -> TableIndex
indexOnColumnWithMethod column method =
  tblIndex
    { idxColumns = [column]
    , idxMethod = method
    }

indexOnColumns :: [IndexColumn] -> TableIndex
indexOnColumns columns = tblIndex {idxColumns = columns}

-- | Create an index on the given columns with the specified method.  No checks
-- are made that the method is appropriate for the type of the column;
-- cf. [the PostgreSQL manual](https://www.postgresql.org/docs/current/static/indexes-multicolumn.html).
indexOnColumnsWithMethod :: [IndexColumn] -> IndexMethod -> TableIndex
indexOnColumnsWithMethod columns method =
  tblIndex
    { idxColumns = columns
    , idxMethod = method
    }

uniqueIndexOnColumn :: IndexColumn -> TableIndex
uniqueIndexOnColumn column =
  TableIndex
    { idxColumns = [column]
    , idxInclude = []
    , idxMethod = BTree
    , idxUnique = True
    , idxWhere = Nothing
    , idxNotDistinctNulls = False
    }

uniqueIndexOnColumns :: [IndexColumn] -> TableIndex
uniqueIndexOnColumns columns =
  TableIndex
    { idxColumns = columns
    , idxInclude = []
    , idxMethod = BTree
    , idxUnique = True
    , idxWhere = Nothing
    , idxNotDistinctNulls = False
    }

uniqueIndexOnColumnWithCondition :: IndexColumn -> RawSQL () -> TableIndex
uniqueIndexOnColumnWithCondition column whereC =
  TableIndex
    { idxColumns = [column]
    , idxInclude = []
    , idxMethod = BTree
    , idxUnique = True
    , idxWhere = Just whereC
    , idxNotDistinctNulls = False
    }

-- | Canonical auto-generated name for an index.
indexName :: RawSQL () -> TableIndex -> RawSQL ()
indexName tname TableIndex {..} =
  asText (T.take 63) $
    mconcat
      [ if idxUnique then "unique_idx__" else "idx__"
      , tname
      , "__"
      , mintercalate "__" $ map (\(IndexColumn col _ q) -> renderIdent q col) idxColumns
      , if null idxInclude
          then ""
          else "$$" <> mintercalate "__" (map (\(IncludeColumn col q) -> renderIdent q col) idxInclude)
      , maybe "" (("__" <>) . hashWhere) idxWhere
      ]
  where
    renderIdent q = asText sanitize . quoteWhen q
    -- See http://www.postgresql.org/docs/9.4/static/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS.
    -- Remove all unallowed characters and replace them by at most one adjacent dollar sign.
    sanitize = T.pack . foldr go [] . T.unpack
      where
        go c acc =
          if isAlphaNum c || c == '_'
            then c : acc
            else case acc of
              ('$' : _) -> acc
              _ -> '$' : acc
    -- hash WHERE clause and add it to index name so that indexes
    -- with the same columns, but different constraints can coexist
    hashWhere =
      asText $
        T.decodeUtf8
          . B16.encode
          . BS.take 10
          . RIPEMD160.hash
          . T.encodeUtf8

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
sqlCreateIndex_ concurrently tname idx@TableIndex {..} =
  mconcat
    [ "CREATE"
    , if idxUnique then " UNIQUE" else ""
    , " INDEX "
    , if concurrently then "CONCURRENTLY " else ""
    , indexName tname idx
    , " ON" <+> tname
    , " USING" <+> rawSQL (T.pack . show $ idxMethod) () <+> "("
    , mintercalate
        ", "
        ( map
            ( \case
                IndexColumn col Nothing q -> quoteWhen q col
                IndexColumn col (Just opclass) q -> quoteWhen q col <+> opclass
            )
            idxColumns
        )
    , ")"
    , if null idxInclude
        then ""
        else
          " INCLUDE ("
            <> mintercalate ", " (map (\(IncludeColumn c q) -> quoteWhen q c) idxInclude)
            <> ")"
    , if idxUnique && idxNotDistinctNulls
        then " NULLS NOT DISTINCT"
        else ""
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
