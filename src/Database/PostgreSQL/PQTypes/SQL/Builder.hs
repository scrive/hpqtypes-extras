{- |

Module "Database.PostgreSQL.PQTypes.SQL.Builder" offers a nice
monadic DSL for building SQL statements on the fly. Some examples:

>>> :{
sqlSelect "documents" $ do
  sqlResult "id"
  sqlResult "title"
  sqlResult "mtime"
  sqlOrderBy "documents.mtime DESC"
  sqlWhereILike "documents.title" "%pattern%"
:}
SQL " SELECT  id, title, mtime FROM documents WHERE (documents.title ILIKE <\"%pattern%\">)    ORDER BY documents.mtime DESC  "

@SQL.Builder@ supports SELECT as 'sqlSelect' and data manipulation using
'sqlInsert', 'sqlInsertSelect', 'sqlDelete' and 'sqlUpdate'.

>>> import Data.Time
>>> let title = "title" :: String
>>> let ctime  = read "2020-01-01 00:00:00 UTC" :: UTCTime
>>> :{
sqlInsert "documents" $ do
  sqlSet "title" title
  sqlSet "ctime" ctime
  sqlResult "id"
:}
SQL " INSERT INTO documents (title, ctime) VALUES (<\"title\">, <2020-01-01 00:00:00 UTC>)  RETURNING id"

The 'sqlInsertSelect' is particulary interesting as it supports INSERT
of values taken from a SELECT clause from same or even different
tables.

There is a possibility to do multiple inserts at once. Data given by
'sqlSetList' will be inserted multiple times, data given by 'sqlSet'
will be multiplied as many times as needed to cover all inserted rows
(it is common to all rows). If you use multiple 'sqlSetList' then
lists will be made equal in length by appending @DEFAULT@ as fill
element.

>>> :{
sqlInsert "documents" $ do
  sqlSet "ctime" ctime
  sqlSetList "title" ["title1", "title2", "title3"]
  sqlResult "id"
:}
SQL " INSERT INTO documents (ctime, title) VALUES (<2020-01-01 00:00:00 UTC>, <\"title1\">) , (<2020-01-01 00:00:00 UTC>, <\"title2\">) , (<2020-01-01 00:00:00 UTC>, <\"title3\">)  RETURNING id"

The above will insert 3 new documents.

@SQL.Builder@ provides quite a lot of SQL magic, including @ORDER BY@ as
'sqlOrderBy', @GROUP BY@ as 'sqlGroupBy'.

>>> :{
sqlSelect "documents" $ do
  sqlResult "id"
  sqlResult "title"
  sqlResult "mtime"
  sqlOrderBy "documents.mtime DESC"
  sqlOrderBy "documents.title"
  sqlGroupBy "documents.status"
  sqlJoinOn "users" "documents.user_id = users.id"
  sqlWhere $ mkSQL "documents.title ILIKE" <?> "%pattern%"
:}
SQL " SELECT  id, title, mtime FROM documents  JOIN  users  ON  documents.user_id = users.id WHERE (documents.title ILIKE <\"%pattern%\">)  GROUP BY documents.status  ORDER BY documents.mtime DESC, documents.title  "

Joins are done by 'sqlJoinOn', 'sqlLeftJoinOn', 'sqlRightJoinOn',
'sqlJoinOn', 'sqlFullJoinOn'. If everything fails use 'sqlJoin' and
'sqlFrom' to set join clause as string. Support for a join grammars as
some kind of abstract syntax data type is lacking.

>>> :{
sqlDelete "mails" $ do
  sqlWhere "id > 67"
:}
SQL " DELETE FROM mails  WHERE (id > 67) "

>>> :{
sqlUpdate "document_tags" $ do
  sqlSet "value" (123 :: Int)
  sqlWhere "name = 'abc'"
:}
SQL " UPDATE document_tags SET value=<123>  WHERE (name = 'abc') "

-}

-- TODO: clean this up, add more documentation.

module Database.PostgreSQL.PQTypes.SQL.Builder
  ( sqlWhere
  , sqlWhereEq
  , sqlWhereEqSql
  , sqlWhereNotEq
  , sqlWhereIn
  , sqlWhereInSql
  , sqlWhereNotIn
  , sqlWhereNotInSql
  , sqlWhereExists
  , sqlWhereNotExists
  , sqlWhereLike
  , sqlWhereILike
  , sqlWhereIsNULL
  , sqlWhereIsNotNULL

  , sqlFrom
  , sqlJoin
  , sqlJoinOn
  , sqlLeftJoinOn
  , sqlRightJoinOn
  , sqlFullJoinOn
  , sqlOnConflictDoNothing
  , sqlOnConflictOnColumns
  , sqlOnConflictOnColumnsDoNothing
  , sqlSet
  , sqlSetInc
  , sqlSetList
  , sqlSetListWithDefaults
  , sqlSetCmd
  , sqlSetCmdList
  , sqlCopyColumn
  , sqlResult
  , sqlOrderBy
  , sqlGroupBy
  , sqlHaving
  , sqlOffset
  , sqlLimit
  , sqlDistinct
  , sqlWith
  , sqlUnion

  , sqlSelect
  , sqlSelect2
  , SqlSelect(..)
  , sqlInsert
  , SqlInsert(..)
  , sqlInsertSelect
  , SqlInsertSelect(..)
  , sqlUpdate
  , SqlUpdate(..)
  , sqlDelete
  , SqlDelete(..)

  , sqlWhereAny

  , SqlResult
  , SqlSet
  , SqlFrom
  , SqlWhere
  , SqlOrderBy
  , SqlGroupByHaving
  , SqlOffsetLimit
  , SqlDistinct

  , SqlCondition(..)
  , sqlGetWhereConditions

  , Sqlable(..)
  , sqlOR
  , sqlConcatComma
  , sqlConcatAND
  , sqlConcatOR
  , parenthesize
  , AscDesc(..)
  )
  where

import Control.Exception.Lifted as E
import Control.Monad.Catch
import Control.Monad.State
import Control.Monad.Trans.Control
import Data.List
import Data.Maybe
import Data.Monoid
import Data.Monoid.Utils
import Data.String
import Data.Typeable
import Database.PostgreSQL.PQTypes
import Prelude
import Safe (atMay)
import qualified Text.JSON.Gen as JSON

class Sqlable a where
  toSQLCommand :: a -> SQL

instance Sqlable SQL where
  toSQLCommand = id

smintercalate :: (IsString m, Monoid m) => m -> [m] -> m
smintercalate m = mintercalate $ mconcat [mspace, m, mspace]

sqlOR :: SQL -> SQL -> SQL
sqlOR s1 s2 = sqlConcatOR [s1, s2]

sqlConcatComma :: [SQL] -> SQL
sqlConcatComma = mintercalate ", "

sqlConcatAND :: [SQL] -> SQL
sqlConcatAND = smintercalate "AND" . map parenthesize

sqlConcatOR :: [SQL] -> SQL
sqlConcatOR = smintercalate "OR" . map parenthesize

parenthesize :: SQL -> SQL
parenthesize s = "(" <> s <> ")"

-- | 'AscDesc' marks ORDER BY order as ascending or descending.
-- Conversion to SQL adds DESC marker to descending and no marker
-- to ascending order.
data AscDesc a = Asc a | Desc a
  deriving (Eq, Show)

data Multiplicity a = Single a | Many [a]
  deriving (Eq, Ord, Show, Typeable)

-- | 'SqlCondition' are clauses that are part of the WHERE block in
-- SQL statements. Each statement has a list of conditions, all of
-- them must be fulfilled.  Sometimes we need to inspect internal
-- structure of a condition. For now it seems that the only
-- interesting case is EXISTS (SELECT ...), because that internal
-- SELECT can have explainable clauses.
data SqlCondition = SqlPlainCondition SQL
                  | SqlExistsCondition SqlSelect
                    deriving (Typeable, Show)

instance Sqlable SqlCondition where
  toSQLCommand (SqlPlainCondition a) = a
  toSQLCommand (SqlExistsCondition a) = "EXISTS (" <> toSQLCommand (a { sqlSelectResult = ["TRUE"] }) <> ")"

data SqlSelect = SqlSelect
  { sqlSelectFrom     :: SQL
  , sqlSelectUnion    :: [SQL]
  , sqlSelectDistinct :: Bool
  , sqlSelectResult   :: [SQL]
  , sqlSelectWhere    :: [SqlCondition]
  , sqlSelectOrderBy  :: [SQL]
  , sqlSelectGroupBy  :: [SQL]
  , sqlSelectHaving   :: [SQL]
  , sqlSelectOffset   :: Integer
  , sqlSelectLimit    :: Integer
  , sqlSelectWith     :: [(SQL, SQL)]
  }

data SqlUpdate = SqlUpdate
  { sqlUpdateWhat   :: SQL
  , sqlUpdateFrom   :: SQL
  , sqlUpdateWhere  :: [SqlCondition]
  , sqlUpdateSet    :: [(SQL,SQL)]
  , sqlUpdateResult :: [SQL]
  , sqlUpdateWith   :: [(SQL, SQL)]
  }

data SqlInsert = SqlInsert
  { sqlInsertWhat       :: SQL
  , sqlInsertOnConflict :: Maybe (SQL, Maybe SQL)
  , sqlInsertSet        :: [(SQL, Multiplicity SQL)]
  , sqlInsertResult     :: [SQL]
  , sqlInsertWith       :: [(SQL, SQL)]
  }

data SqlInsertSelect = SqlInsertSelect
  { sqlInsertSelectWhat     :: SQL
  , sqlInsertSelectDistinct :: Bool
  , sqlInsertSelectSet      :: [(SQL, SQL)]
  , sqlInsertSelectResult   :: [SQL]
  , sqlInsertSelectFrom     :: SQL
  , sqlInsertSelectWhere    :: [SqlCondition]
  , sqlInsertSelectOrderBy  :: [SQL]
  , sqlInsertSelectGroupBy  :: [SQL]
  , sqlInsertSelectHaving   :: [SQL]
  , sqlInsertSelectOffset   :: Integer
  , sqlInsertSelectLimit    :: Integer
  , sqlInsertSelectWith     :: [(SQL, SQL)]
  }

data SqlDelete = SqlDelete
  { sqlDeleteFrom   :: SQL
  , sqlDeleteUsing  :: SQL
  , sqlDeleteWhere  :: [SqlCondition]
  , sqlDeleteResult :: [SQL]
  , sqlDeleteWith   :: [(SQL, SQL)]
  }

-- | This is not exported and is used as an implementation detail in
-- 'sqlWhereAll'.
data SqlAll = SqlAll
  { sqlAllWhere :: [SqlCondition]
  }

instance Show SqlSelect where
  show = show . toSQLCommand

instance Show SqlInsert where
  show = show . toSQLCommand

instance Show SqlInsertSelect where
  show = show . toSQLCommand

instance Show SqlUpdate where
  show = show . toSQLCommand

instance Show SqlDelete where
  show = show . toSQLCommand

instance Show SqlAll where
  show = show . toSQLCommand

emitClause :: Sqlable sql => SQL -> sql -> SQL
emitClause name s = case toSQLCommand s of
  sql
    | isSqlEmpty sql -> ""
    | otherwise   -> name <+> sql

emitClausesSep :: SQL -> SQL -> [SQL] -> SQL
emitClausesSep _name _sep [] = mempty
emitClausesSep name sep sqls = name <+> smintercalate sep (filter (not . isSqlEmpty) $ map parenthesize sqls)

emitClausesSepComma :: SQL -> [SQL] -> SQL
emitClausesSepComma _name [] = mempty
emitClausesSepComma name sqls = name <+> sqlConcatComma (filter (not . isSqlEmpty) sqls)

instance IsSQL SqlSelect where
  withSQL = withSQL . toSQLCommand

instance IsSQL SqlInsert where
  withSQL = withSQL . toSQLCommand

instance IsSQL SqlInsertSelect where
  withSQL = withSQL . toSQLCommand

instance IsSQL SqlUpdate where
  withSQL = withSQL . toSQLCommand

instance IsSQL SqlDelete where
  withSQL = withSQL . toSQLCommand

instance Sqlable SqlSelect where
  toSQLCommand cmd = smconcat
    [ emitClausesSepComma "WITH" $
      map (\(name,command) -> name <+> "AS" <+> parenthesize command) (sqlSelectWith cmd)
    , if hasUnion
      then emitClausesSep "" "UNION" (mainSelectClause : sqlSelectUnion cmd)
      else mainSelectClause
    , emitClausesSepComma "GROUP BY" (sqlSelectGroupBy cmd)
    , emitClausesSep "HAVING" "AND" (sqlSelectHaving cmd)
    , orderByClause
    , if sqlSelectOffset cmd > 0
      then unsafeSQL ("OFFSET " ++ show (sqlSelectOffset cmd))
      else ""
    , if sqlSelectLimit cmd >= 0
      then limitClause
      else ""
    ]
    where
      mainSelectClause = smconcat
        [ "SELECT" <+> (if sqlSelectDistinct cmd then "DISTINCT" else mempty)
        , sqlConcatComma (sqlSelectResult cmd)
        , emitClause "FROM" (sqlSelectFrom cmd)
        , emitClausesSep "WHERE" "AND" (map toSQLCommand $ sqlSelectWhere cmd)
        -- If there's a union, the result is sorted and has a limit, applying
        -- the order and limit to the main subquery won't reduce the overall
        -- query result, but might reduce its processing time.
        , if hasUnion && not (null $ sqlSelectOrderBy cmd) && sqlSelectLimit cmd >= 0
          then smconcat [orderByClause, limitClause]
          else ""
        ]

      hasUnion      = not . null $ sqlSelectUnion cmd
      orderByClause = emitClausesSepComma "ORDER BY" $ sqlSelectOrderBy cmd
      limitClause   = unsafeSQL $ "LIMIT" <+> show (sqlSelectLimit cmd)

instance Sqlable SqlInsert where
  toSQLCommand cmd =
    emitClausesSepComma "WITH" (map (\(name,command) -> name <+> "AS" <+> parenthesize command) (sqlInsertWith cmd)) <+>
    "INSERT INTO" <+> sqlInsertWhat cmd <+>
    parenthesize (sqlConcatComma (map fst (sqlInsertSet cmd))) <+>
    emitClausesSep "VALUES" "," (map sqlConcatComma (transpose (map (makeLongEnough . snd) (sqlInsertSet cmd)))) <+>
    emitClauseOnConflict (sqlInsertOnConflict cmd) <+>
    emitClausesSepComma "RETURNING" (sqlInsertResult cmd)
   where
     emitClauseOnConflict = \case
       Nothing                   -> ""
       Just (condition, maction) -> emitClause "ON CONFLICT" $
         condition <+> "DO" <+> fromMaybe "NOTHING" maction

     -- this is the longest list of values
     longest = maximum (1 : (map (lengthOfEither . snd) (sqlInsertSet cmd)))
     lengthOfEither (Single _) = 1
     lengthOfEither (Many x)   = length x
     makeLongEnough (Single x) = take longest (repeat x)
     makeLongEnough (Many x)   = take longest (x ++ repeat "DEFAULT")

instance Sqlable SqlInsertSelect where
  toSQLCommand cmd = smconcat
    -- WITH clause needs to be at the top level, so we emit it here and not
    -- include it in the SqlSelect below.
    [ emitClausesSepComma "WITH" $
      map (\(name,command) -> name <+> "AS" <+> parenthesize command) (sqlInsertSelectWith cmd)
    , "INSERT INTO" <+> sqlInsertSelectWhat cmd
    , parenthesize . sqlConcatComma . map fst $ sqlInsertSelectSet cmd
    , parenthesize . toSQLCommand $ SqlSelect { sqlSelectFrom    = sqlInsertSelectFrom cmd
                                              , sqlSelectUnion   = []
                                              , sqlSelectDistinct = sqlInsertSelectDistinct cmd
                                              , sqlSelectResult  = fmap snd $ sqlInsertSelectSet cmd
                                              , sqlSelectWhere   = sqlInsertSelectWhere cmd
                                              , sqlSelectOrderBy = sqlInsertSelectOrderBy cmd
                                              , sqlSelectGroupBy = sqlInsertSelectGroupBy cmd
                                              , sqlSelectHaving  = sqlInsertSelectHaving cmd
                                              , sqlSelectOffset  = sqlInsertSelectOffset cmd
                                              , sqlSelectLimit   = sqlInsertSelectLimit cmd
                                              , sqlSelectWith    = []
                                              }
    , emitClausesSepComma "RETURNING" $ sqlInsertSelectResult cmd
    ]

instance Sqlable SqlUpdate where
  toSQLCommand cmd =
    emitClausesSepComma "WITH" (map (\(name,command) -> name <+> "AS" <+> parenthesize command) (sqlUpdateWith cmd)) <+>
    "UPDATE" <+> sqlUpdateWhat cmd <+> "SET" <+>
    sqlConcatComma (map (\(name, command) -> name <> "=" <> command) (sqlUpdateSet cmd)) <+>
    emitClause "FROM" (sqlUpdateFrom cmd) <+>
    emitClausesSep "WHERE" "AND" (map toSQLCommand $ sqlUpdateWhere cmd) <+>
    emitClausesSepComma "RETURNING" (sqlUpdateResult cmd)

instance Sqlable SqlDelete where
  toSQLCommand cmd =
    emitClausesSepComma "WITH" (map (\(name,command) -> name <+> "AS" <+> parenthesize command) (sqlDeleteWith cmd)) <+>
    "DELETE FROM" <+> sqlDeleteFrom cmd <+>
    emitClause "USING" (sqlDeleteUsing cmd) <+>
        emitClausesSep "WHERE" "AND" (map toSQLCommand $ sqlDeleteWhere cmd) <+>
    emitClausesSepComma "RETURNING" (sqlDeleteResult cmd)

instance Sqlable SqlAll where
  toSQLCommand cmd | null (sqlAllWhere cmd) = "TRUE"
  toSQLCommand cmd =
    "(" <+> smintercalate "AND" (map (parenthesize . toSQLCommand) (sqlAllWhere cmd)) <+> ")"


sqlSelect :: SQL -> State SqlSelect () -> SqlSelect
sqlSelect table refine =
  execState refine (SqlSelect table [] False [] [] [] [] [] 0 (-1) [])

sqlSelect2 :: SQL -> State SqlSelect () -> SqlSelect
sqlSelect2 from refine =
  execState refine (SqlSelect from [] False [] [] [] [] [] 0 (-1) [])

sqlInsert :: SQL -> State SqlInsert () -> SqlInsert
sqlInsert table refine =
  execState refine (SqlInsert table Nothing mempty [] [])

sqlInsertSelect :: SQL -> SQL -> State SqlInsertSelect () -> SqlInsertSelect
sqlInsertSelect table from refine =
  execState refine (SqlInsertSelect
                    { sqlInsertSelectWhat    = table
                    , sqlInsertSelectDistinct = False
                    , sqlInsertSelectSet     = []
                    , sqlInsertSelectResult  = []
                    , sqlInsertSelectFrom    = from
                    , sqlInsertSelectWhere   = []
                    , sqlInsertSelectOrderBy = []
                    , sqlInsertSelectGroupBy = []
                    , sqlInsertSelectHaving  = []
                    , sqlInsertSelectOffset  = 0
                    , sqlInsertSelectLimit   = -1
                    , sqlInsertSelectWith    = []
                    })

sqlUpdate :: SQL -> State SqlUpdate () -> SqlUpdate
sqlUpdate table refine =
  execState refine (SqlUpdate table mempty [] [] [] [])

sqlDelete :: SQL -> State SqlDelete () -> SqlDelete
sqlDelete table refine =
  execState refine (SqlDelete  { sqlDeleteFrom   = table
                               , sqlDeleteUsing  = mempty
                               , sqlDeleteWhere  = []
                               , sqlDeleteResult = []
                               , sqlDeleteWith   = []
                               })

class SqlWith a where
  sqlWith1 :: a -> SQL -> SQL -> a


instance SqlWith SqlSelect where
  sqlWith1 cmd name sql = cmd { sqlSelectWith = sqlSelectWith cmd ++ [(name,sql)] }

instance SqlWith SqlInsertSelect where
  sqlWith1 cmd name sql = cmd { sqlInsertSelectWith = sqlInsertSelectWith cmd ++ [(name,sql)] }

instance SqlWith SqlUpdate where
  sqlWith1 cmd name sql = cmd { sqlUpdateWith = sqlUpdateWith cmd ++ [(name,sql)] }

instance SqlWith SqlDelete where
  sqlWith1 cmd name sql = cmd { sqlDeleteWith = sqlDeleteWith cmd ++ [(name,sql)] }

sqlWith :: (MonadState v m, SqlWith v, Sqlable s) => SQL -> s -> m ()
sqlWith name sql = modify (\cmd -> sqlWith1 cmd name (toSQLCommand sql))

-- | Note: WHERE clause of the main SELECT is treated specially, i.e. it only
-- applies to the main SELECT, not the whole union.
sqlUnion :: (MonadState SqlSelect m, Sqlable sql) => [sql] -> m ()
sqlUnion sqls = modify (\cmd -> cmd { sqlSelectUnion = map toSQLCommand sqls })

class SqlWhere a where
  sqlWhere1 :: a -> SqlCondition -> a
  sqlGetWhereConditions :: a -> [SqlCondition]

instance SqlWhere SqlSelect where
  sqlWhere1 cmd cond = cmd { sqlSelectWhere = sqlSelectWhere cmd ++ [cond] }
  sqlGetWhereConditions = sqlSelectWhere

instance SqlWhere SqlInsertSelect where
  sqlWhere1 cmd cond = cmd { sqlInsertSelectWhere = sqlInsertSelectWhere cmd ++ [cond] }
  sqlGetWhereConditions = sqlInsertSelectWhere

instance SqlWhere SqlUpdate where
  sqlWhere1 cmd cond = cmd { sqlUpdateWhere = sqlUpdateWhere cmd ++ [cond] }
  sqlGetWhereConditions = sqlUpdateWhere

instance SqlWhere SqlDelete where
  sqlWhere1 cmd cond = cmd { sqlDeleteWhere = sqlDeleteWhere cmd ++ [cond] }
  sqlGetWhereConditions = sqlDeleteWhere

instance SqlWhere SqlAll where
  sqlWhere1 cmd cond = cmd { sqlAllWhere = sqlAllWhere cmd ++ [cond] }
  sqlGetWhereConditions = sqlAllWhere

-- | The @WHERE@ part of an SQL query. See above for a usage
-- example. See also 'SqlCondition'.
sqlWhere :: (MonadState v m, SqlWhere v) => SQL -> m ()
sqlWhere sql = modify (\cmd -> sqlWhere1 cmd (SqlPlainCondition sql))

sqlWhereEq :: (MonadState v m, SqlWhere v, Show a, ToSQL a) => SQL -> a -> m ()
sqlWhereEq name value = sqlWhere $ name <+> "=" <?> value

sqlWhereEqSql :: (MonadState v m, SqlWhere v, Sqlable sql) => SQL -> sql -> m ()
sqlWhereEqSql name1 name2 = sqlWhere $ name1 <+> "=" <+> toSQLCommand name2

sqlWhereNotEq :: (MonadState v m, SqlWhere v, Show a, ToSQL a) => SQL -> a -> m ()
sqlWhereNotEq name value = sqlWhere $ name <+> "<>" <?> value

sqlWhereLike :: (MonadState v m, SqlWhere v, Show a, ToSQL a) => SQL -> a -> m ()
sqlWhereLike name value = sqlWhere $ name <+> "LIKE" <?> value

sqlWhereILike :: (MonadState v m, SqlWhere v, Show a, ToSQL a) => SQL -> a -> m ()
sqlWhereILike name value = sqlWhere  $ name <+> "ILIKE" <?> value

sqlWhereIn :: (MonadState v m, SqlWhere v, Show a, ToSQL a) => SQL -> [a] -> m ()
sqlWhereIn _name [] = sqlWhere "FALSE"
sqlWhereIn name [value] = sqlWhereEq name value
sqlWhereIn name values = do
  -- Unpack the array to give query optimizer more options.
  sqlWhere $ name <+> "IN (SELECT UNNEST(" <?> Array1 values <+> "))"

sqlWhereInSql :: (MonadState v m, Sqlable a, SqlWhere v) => SQL -> a -> m ()
sqlWhereInSql name sql = sqlWhere $ name <+> "IN" <+> parenthesize (toSQLCommand sql)

sqlWhereNotIn :: (MonadState v m, SqlWhere v, Show a, ToSQL a) => SQL -> [a] -> m ()
sqlWhereNotIn _name [] = sqlWhere "TRUE"
sqlWhereNotIn name [value] = sqlWhereNotEq name value
sqlWhereNotIn name values = sqlWhere $ name <+> "NOT IN (SELECT UNNEST(" <?> Array1 values <+> "))"

sqlWhereNotInSql :: (MonadState v m, Sqlable a, SqlWhere v) => SQL -> a -> m ()
sqlWhereNotInSql name sql = sqlWhere $ name <+> "NOT IN" <+> parenthesize (toSQLCommand sql)

sqlWhereExists :: (MonadState v m, SqlWhere v) => SqlSelect -> m ()
sqlWhereExists sql = do
  modify (\cmd -> sqlWhere1 cmd (SqlExistsCondition sql))

sqlWhereNotExists :: (MonadState v m, SqlWhere v) => SqlSelect -> m ()
sqlWhereNotExists sqlSelectD = do
  sqlWhere ("NOT EXISTS (" <+> toSQLCommand (sqlSelectD { sqlSelectResult = ["TRUE"] }) <+> ")")

sqlWhereIsNULL :: (MonadState v m, SqlWhere v) => SQL -> m ()
sqlWhereIsNULL col = sqlWhere $ col <+> "IS NULL"

sqlWhereIsNotNULL :: (MonadState v m, SqlWhere v) => SQL -> m ()
sqlWhereIsNotNULL col = sqlWhere $ col <+> "IS NOT NULL"

-- | Add a condition in the WHERE statement that holds if any of the given
-- condition holds.
sqlWhereAny :: (MonadState v m, SqlWhere v) => [State SqlAll ()] -> m ()
sqlWhereAny = sqlWhere . sqlWhereAnyImpl

sqlWhereAnyImpl :: [State SqlAll ()] -> SQL
sqlWhereAnyImpl [] = "FALSE"
sqlWhereAnyImpl l =
  "(" <+> smintercalate "OR" (map (parenthesize . toSQLCommand
                                   . flip execState (SqlAll [])) l) <+> ")"

class SqlFrom a where
  sqlFrom1 :: a -> SQL -> a

instance SqlFrom SqlSelect where
  sqlFrom1 cmd sql = cmd { sqlSelectFrom = sqlSelectFrom cmd <+> sql }

instance SqlFrom SqlInsertSelect where
  sqlFrom1 cmd sql = cmd { sqlInsertSelectFrom = sqlInsertSelectFrom cmd <+> sql }

instance SqlFrom SqlUpdate where
  sqlFrom1 cmd sql = cmd { sqlUpdateFrom = sqlUpdateFrom cmd <+> sql }

instance SqlFrom SqlDelete where
  sqlFrom1 cmd sql = cmd { sqlDeleteUsing = sqlDeleteUsing cmd <+> sql }

sqlFrom :: (MonadState v m, SqlFrom v) => SQL -> m ()
sqlFrom sql = modify (\cmd -> sqlFrom1 cmd sql)

sqlJoin :: (MonadState v m, SqlFrom v) => SQL -> m ()
sqlJoin table = sqlFrom (", " <+> table)

sqlJoinOn :: (MonadState v m, SqlFrom v) => SQL -> SQL -> m ()
sqlJoinOn table condition = sqlFrom (" JOIN " <+>
                                     table <+>
                                     " ON " <+>
                                     condition)

sqlLeftJoinOn :: (MonadState v m, SqlFrom v) => SQL -> SQL -> m ()
sqlLeftJoinOn table condition = sqlFrom (" LEFT JOIN " <+>
                                         table <+>
                                         " ON " <+>
                                         condition)

sqlRightJoinOn :: (MonadState v m, SqlFrom v) => SQL -> SQL -> m ()
sqlRightJoinOn table condition = sqlFrom (" RIGHT JOIN " <+>
                                          table <+>
                                          " ON " <+>
                                          condition)

sqlFullJoinOn :: (MonadState v m, SqlFrom v) => SQL -> SQL -> m ()
sqlFullJoinOn table condition = sqlFrom (" FULL JOIN " <+>
                                         table <+>
                                         " ON " <+>
                                         condition)

class SqlSet a where
  sqlSet1 :: a -> SQL -> SQL -> a

instance SqlSet SqlUpdate where
  sqlSet1 cmd name v = cmd { sqlUpdateSet = sqlUpdateSet cmd ++ [(name, v)] }

instance SqlSet SqlInsert where
  sqlSet1 cmd name v = cmd { sqlInsertSet = sqlInsertSet cmd ++ [(name, Single v)] }

instance SqlSet SqlInsertSelect where
  sqlSet1 cmd name v = cmd { sqlInsertSelectSet = sqlInsertSelectSet cmd ++ [(name, v)] }

sqlOnConflictDoNothing :: MonadState SqlInsert m => m ()
sqlOnConflictDoNothing = modify $ \cmd -> cmd
  { sqlInsertOnConflict = Just ("", Nothing)
  }

sqlOnConflictOnColumns
  :: Sqlable sql => MonadState SqlInsert m => [SQL] -> sql -> m ()
sqlOnConflictOnColumns columns sql = modify $ \cmd -> cmd
  { sqlInsertOnConflict = Just (parenthesize $ sqlConcatComma columns, Just $ toSQLCommand sql)
  }

sqlOnConflictOnColumnsDoNothing :: MonadState SqlInsert m => [SQL] -> m ()
sqlOnConflictOnColumnsDoNothing columns = modify $ \cmd -> cmd
  { sqlInsertOnConflict = Just (parenthesize $ sqlConcatComma columns, Nothing)
  }

sqlSetCmd :: (MonadState v m, SqlSet v) => SQL -> SQL -> m ()
sqlSetCmd name sql = modify (\cmd -> sqlSet1 cmd name sql)

sqlSetCmdList :: (MonadState SqlInsert m) => SQL -> [SQL] -> m ()
sqlSetCmdList name as = modify (\cmd -> cmd { sqlInsertSet = sqlInsertSet cmd ++ [(name, Many as)] })

sqlSet :: (MonadState v m, SqlSet v, Show a, ToSQL a) => SQL -> a -> m ()
sqlSet name a = sqlSetCmd name (sqlParam a)

sqlSetInc :: (MonadState v m, SqlSet v) => SQL -> m ()
sqlSetInc name = sqlSetCmd name $ name <+> "+ 1"

sqlSetList :: (MonadState SqlInsert m, Show a, ToSQL a) => SQL -> [a] -> m ()
sqlSetList name as = sqlSetCmdList name (map sqlParam as)

sqlSetListWithDefaults :: (MonadState SqlInsert m, Show a, ToSQL a) => SQL -> [Maybe a] -> m ()
sqlSetListWithDefaults name as = sqlSetCmdList name (map (maybe "DEFAULT" sqlParam) as)

sqlCopyColumn :: (MonadState v m, SqlSet v) => SQL -> m ()
sqlCopyColumn column = sqlSetCmd column column

class SqlResult a where
  sqlResult1 :: a -> SQL -> a

instance SqlResult SqlSelect where
  sqlResult1 cmd sql = cmd { sqlSelectResult = sqlSelectResult cmd ++ [sql] }

instance SqlResult SqlInsert where
  sqlResult1 cmd sql = cmd { sqlInsertResult = sqlInsertResult cmd ++ [sql] }

instance SqlResult SqlInsertSelect where
  sqlResult1 cmd sql = cmd { sqlInsertSelectResult = sqlInsertSelectResult cmd ++ [sql] }

instance SqlResult SqlUpdate where
  sqlResult1 cmd sql = cmd { sqlUpdateResult = sqlUpdateResult cmd ++ [sql] }



sqlResult :: (MonadState v m, SqlResult v) => SQL -> m ()
sqlResult sql = modify (\cmd -> sqlResult1 cmd sql)

class SqlOrderBy a where
  sqlOrderBy1 :: a -> SQL -> a

instance SqlOrderBy SqlSelect where
  sqlOrderBy1 cmd sql = cmd { sqlSelectOrderBy = sqlSelectOrderBy cmd ++ [sql] }

instance SqlOrderBy SqlInsertSelect where
  sqlOrderBy1 cmd sql = cmd { sqlInsertSelectOrderBy = sqlInsertSelectOrderBy cmd ++ [sql] }


sqlOrderBy :: (MonadState v m, SqlOrderBy v) => SQL -> m ()
sqlOrderBy sql = modify (\cmd -> sqlOrderBy1 cmd sql)

class SqlGroupByHaving a where
  sqlGroupBy1 :: a -> SQL -> a
  sqlHaving1 :: a -> SQL -> a

instance SqlGroupByHaving SqlSelect where
  sqlGroupBy1 cmd sql = cmd { sqlSelectGroupBy = sqlSelectGroupBy cmd ++ [sql] }
  sqlHaving1 cmd sql = cmd { sqlSelectHaving = sqlSelectHaving cmd ++ [sql] }

instance SqlGroupByHaving SqlInsertSelect where
  sqlGroupBy1 cmd sql = cmd { sqlInsertSelectGroupBy = sqlInsertSelectGroupBy cmd ++ [sql] }
  sqlHaving1 cmd sql = cmd { sqlInsertSelectHaving = sqlInsertSelectHaving cmd ++ [sql] }

sqlGroupBy :: (MonadState v m, SqlGroupByHaving v) => SQL -> m ()
sqlGroupBy sql = modify (\cmd -> sqlGroupBy1 cmd sql)

sqlHaving :: (MonadState v m, SqlGroupByHaving v) => SQL -> m ()
sqlHaving sql = modify (\cmd -> sqlHaving1 cmd sql)


class SqlOffsetLimit a where
  sqlOffset1 :: a -> Integer -> a
  sqlLimit1 :: a -> Integer -> a

instance SqlOffsetLimit SqlSelect where
  sqlOffset1 cmd num = cmd { sqlSelectOffset = num }
  sqlLimit1 cmd num = cmd { sqlSelectLimit = num }

instance SqlOffsetLimit SqlInsertSelect where
  sqlOffset1 cmd num = cmd { sqlInsertSelectOffset = num }
  sqlLimit1 cmd num = cmd { sqlInsertSelectLimit = num }

sqlOffset :: (MonadState v m, SqlOffsetLimit v, Integral int) => int -> m ()
sqlOffset val = modify (\cmd -> sqlOffset1 cmd $ toInteger val)

sqlLimit :: (MonadState v m, SqlOffsetLimit v, Integral int) => int -> m ()
sqlLimit val = modify (\cmd -> sqlLimit1 cmd $ toInteger val)

class SqlDistinct a where
  sqlDistinct1 :: a -> a

instance SqlDistinct SqlSelect where
  sqlDistinct1 cmd = cmd { sqlSelectDistinct = True }

instance SqlDistinct SqlInsertSelect where
  sqlDistinct1 cmd = cmd { sqlInsertSelectDistinct = True }

sqlDistinct :: (MonadState v m, SqlDistinct v) => m ()
sqlDistinct = modify (\cmd -> sqlDistinct1 cmd)
