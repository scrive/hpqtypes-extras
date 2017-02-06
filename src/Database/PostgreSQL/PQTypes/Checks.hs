module Database.PostgreSQL.PQTypes.Checks (
    migrateDatabase
  , checkDatabase
  , createTable
  , createDomain
  , MigrateOptions(..)
  ) where

import Control.Applicative ((<$>))
import Control.Monad.Catch
import Control.Monad.Reader
import Data.Int
import Data.Maybe
import Data.Monoid
import Data.Monoid.Utils
import Database.PostgreSQL.PQTypes hiding (def)
import Log
import Prelude
import TextShow
import qualified Data.Foldable as F
import qualified Data.List as L
import qualified Data.Text as T

import Database.PostgreSQL.PQTypes.Model
import Database.PostgreSQL.PQTypes.SQL.Builder
import Database.PostgreSQL.PQTypes.Versions

newtype ValidationResult = ValidationResult [T.Text] -- ^ list of error messages

data MigrateOptions = ForceCommitAfterEveryMigration deriving Eq

instance Monoid ValidationResult where
  mempty = ValidationResult []
  mappend (ValidationResult a) (ValidationResult b) = ValidationResult (a ++ b)

topMessage :: T.Text -> T.Text -> ValidationResult -> ValidationResult
topMessage objtype objname = \case
  ValidationResult [] -> ValidationResult []
  ValidationResult es -> ValidationResult ("There are problems with the" <+> objtype <+> "'" <> objname <> "'" : es)

resultCheck
  :: (MonadLog m, MonadThrow m)
  => ValidationResult
  -> m ()
resultCheck = \case
  ValidationResult [] -> return ()
  ValidationResult msgs -> do
    mapM_ logAttention_ msgs
    error "resultCheck: validation failed"

----------------------------------------

-- | Runs all checks on a database
migrateDatabase
  :: (MonadDB m, MonadLog m, MonadThrow m)
  => [MigrateOptions] -> [Extension] -> [Domain] -> [Table] -> [Migration m]
  -> m ()
migrateDatabase options extensions domains tables migrations = do
  void checkDBTimeZone
  mapM_ checkExtension extensions
  checkDBConsistency options domains (tableVersions : tables) migrations
  resultCheck =<< checkDomainsStructure domains
  resultCheck =<< checkDBStructure (tableVersions : tables)
  checkUnknownTables tables

  -- everything is OK, commit changes
  commit

checkDatabase
  :: (MonadDB m, MonadLog m, MonadThrow m)
  => [Domain] -> [Table]
  -> m ()
checkDatabase domains tables = do
  versions <- mapM checkTableVersion tables
  let tablesWithVersions = zip tables (map (fromMaybe 0) versions)
  resultCheck . mconcat $ checkVersions tablesWithVersions
  resultCheck =<< checkDomainsStructure domains
  resultCheck =<< checkDBStructure (tableVersions : tables)
  -- check initial setups only after database structure is considered
  -- consistent as before that some of the checks may fail internally.
  resultCheck . mconcat =<< checkInitialSetups tables
  where
    checkVersions = map $ \(t@Table{..}, v) -> ValidationResult $ if
      | tblVersion == v -> []
      | v == 0 -> ["Table '" <> tblNameText t <> "' must be created"]
      | otherwise -> ["Table '" <> tblNameText t <> "' must be migrated" <+> showt v <+> "->" <+> showt tblVersion]

    checkInitialSetups = mapM $ \t -> case tblInitialSetup t of
      Nothing -> return $ ValidationResult []
      Just is -> checkInitialSetup is >>= \case
        True  -> return $ ValidationResult []
        False -> return $ ValidationResult ["Initial setup for table '" <> tblNameText t <> "' is not valid"]

-- | Return SQL fragment of current catalog within quotes
currentCatalog :: (MonadDB m, MonadThrow m) => m (RawSQL ())
currentCatalog = do
  runSQL_ "SELECT current_catalog::text"
  dbname <- fetchOne runIdentity
  return $ unsafeSQL $ "\"" ++ dbname ++ "\""

-- | Check for a given extension. We need to read from 'pg_extension'
-- table as Amazon RDS limits usage of 'CREATE EXTENSION IF NOT EXISTS'.
checkExtension :: (MonadDB m, MonadLog m, MonadThrow m) => Extension -> m ()
checkExtension (Extension extension) = do
  logInfo_ $ "Checking for extension '" <> txtExtension <> "'"
  extensionExists <- runQuery01 . sqlSelect "pg_extension" $ do
    sqlResult "TRUE"
    sqlWhereEq "extname" $ unRawSQL extension
  if not extensionExists
    then do
      logInfo_ $ "Creating extension '" <> txtExtension <> "'"
      runSQL_ $ "CREATE EXTENSION IF NOT EXISTS" <+> raw extension
    else logInfo_ $ "Extension '" <> txtExtension <> "' exists"
  where
    txtExtension = unRawSQL extension

-- |  Checks whether database returns timestamps in UTC
checkDBTimeZone :: (MonadDB m, MonadLog m, MonadThrow m) => m Bool
checkDBTimeZone = do
  runSQL_ "SHOW timezone"
  timezone :: String <- fetchOne runIdentity
  if timezone /= "UTC"
    then do
      dbname <- currentCatalog
      logInfo_ $ "Setting '" <> unRawSQL dbname
        <> "' database to return timestamps in UTC"
      runQuery_ $ "ALTER DATABASE" <+> dbname <+> "SET TIMEZONE = 'UTC'"
      return True
    else return False

checkUnknownTables :: (MonadDB m, MonadLog m) => [Table] -> m ()
checkUnknownTables tables = do
  runQuery_ $ sqlSelect "information_schema.tables" $ do
    sqlResult "table_name::text"
    sqlWhere "table_name <> 'table_versions'"
    sqlWhere "table_type = 'BASE TABLE'"
    sqlWhere "table_schema NOT IN ('information_schema','pg_catalog')"
  desc <- fetchMany runIdentity
  let absent = desc L.\\ map (unRawSQL . tblName) tables
  when (not (null absent)) $
    mapM_ (\t -> logInfo_ $ "Unknown table:" <+> t) absent

createTable :: MonadDB m => Bool -> Table -> m ()
createTable withConstraints table@Table{..} = do
  -- Create empty table and add the columns.
  runQuery_ $ sqlCreateTable tblName
  runQuery_ $ sqlAlterTable tblName $ map sqlAddColumn tblColumns
  -- Add indexes.
  forM_ tblIndexes $ runQuery_ . sqlCreateIndex tblName
  -- Add all the other constraints if applicable.
  when withConstraints $ createTableConstraints table
  -- Register the table along with its version.
  runQuery_ . sqlInsert "table_versions" $ do
    sqlSet "name" (tblNameText table)
    sqlSet "version" tblVersion

createTableConstraints :: MonadDB m => Table -> m ()
createTableConstraints Table{..} = when (not $ null addConstraints) $ do
  runQuery_ $ sqlAlterTable tblName addConstraints
  where
    addConstraints = concat [
        [sqlAddPK tblName pk | Just pk <- return tblPrimaryKey]
      , map sqlAddCheck tblChecks
      , map (sqlAddFK tblName) tblForeignKeys
      ]

checkDomainsStructure :: (MonadDB m, MonadThrow m)
                      => [Domain] -> m ValidationResult
checkDomainsStructure defs = fmap mconcat . forM defs $ \def -> do
  runQuery_ . sqlSelect "pg_catalog.pg_type t1" $ do
    sqlResult "t1.typname::text" -- name
    sqlResult "(SELECT pg_catalog.format_type(t2.oid, t2.typtypmod) FROM pg_catalog.pg_type t2 WHERE t2.oid = t1.typbasetype)" -- type
    sqlResult "NOT t1.typnotnull" -- nullable
    sqlResult "t1.typdefault" -- default value
    sqlResult "ARRAY(SELECT c.conname::text FROM pg_catalog.pg_constraint c WHERE c.contypid = t1.oid ORDER by c.oid)" -- constraint names
    sqlResult "ARRAY(SELECT regexp_replace(pg_get_constraintdef(c.oid, true), 'CHECK \\((.*)\\)', '\\1') FROM pg_catalog.pg_constraint c WHERE c.contypid = t1.oid ORDER by c.oid)" -- constraint definitions
    sqlWhereEq "t1.typname" $ unRawSQL $ domName def
  mdom <- fetchMaybe $ \(dname, dtype, nullable, defval, cnames, conds) -> Domain {
    domName = unsafeSQL dname
  , domType = dtype
  , domNullable = nullable
  , domDefault = unsafeSQL <$> defval
  , domChecks = mkChecks $ zipWith (\cname cond -> Check {
      chkName = unsafeSQL cname
    , chkCondition = unsafeSQL cond
    }) (unArray1 cnames) (unArray1 conds)
  }
  return $ case mdom of
    Just dom
      | dom /= def -> topMessage "domain" (unRawSQL $ domName dom) $ mconcat [
          compareAttr dom def "name" domName
        , compareAttr dom def "type" domType
        , compareAttr dom def "nullable" domNullable
        , compareAttr dom def "default" domDefault
        , compareAttr dom def "checks" domChecks
        ]
      | otherwise -> mempty
    Nothing -> ValidationResult ["Domain '" <> unRawSQL (domName def) <> "' doesn't exist in the database"]
  where
    compareAttr :: (Eq a, Show a) => Domain -> Domain -> T.Text -> (Domain -> a) -> ValidationResult
    compareAttr dom def attrname attr
      | attr dom == attr def = ValidationResult []
      | otherwise = ValidationResult ["Attribute '" <> attrname <> "' does not match (database:" <+> T.pack (show $ attr dom) <> ", definition:" <+> T.pack (show $ attr def) <> ")"]

createDomain :: MonadDB m => Domain -> m ()
createDomain dom@Domain{..} = do
  -- create the domain
  runQuery_ $ sqlCreateDomain dom
  -- add constraint checks to the domain
  F.forM_ domChecks $ runQuery_ . sqlAlterDomain domName . sqlAddCheck

-- | Checks whether database is consistent (performs migrations if necessary)
checkDBStructure :: forall m. (MonadDB m, MonadThrow m)
                 => [Table] -> m ValidationResult
checkDBStructure tables = fmap mconcat . forM tables $ \table ->
  -- final checks for table structure, we do this
  -- both when creating stuff and when migrating
  topMessage "table" (tblNameText table) <$> checkTableStructure table
  where
    checkTableStructure :: Table -> m ValidationResult
    checkTableStructure table@Table{..} = do
      -- get table description from pg_catalog as describeTable
      -- mechanism from HDBC doesn't give accurate results
      runQuery_ $ sqlSelect "pg_catalog.pg_attribute a" $ do
        sqlResult "a.attname::text"
        sqlResult "pg_catalog.format_type(a.atttypid, a.atttypmod)"
        sqlResult "NOT a.attnotnull"
        sqlResult . parenthesize . toSQLCommand $
          sqlSelect "pg_catalog.pg_attrdef d" $ do
            sqlResult "pg_catalog.pg_get_expr(d.adbin, d.adrelid)"
            sqlWhere "d.adrelid = a.attrelid"
            sqlWhere "d.adnum = a.attnum"
            sqlWhere "a.atthasdef"
        sqlWhere "a.attnum > 0"
        sqlWhere "NOT a.attisdropped"
        sqlWhereEqSql "a.attrelid" $ sqlGetTableID table
        sqlOrderBy "a.attnum"
      desc <- fetchMany fetchTableColumn
      -- get info about constraints from pg_catalog
      runQuery_ $ sqlGetPrimaryKey table
      pk <- join <$> fetchMaybe fetchPrimaryKey
      runQuery_ $ sqlGetChecks table
      checks <- fetchMany fetchTableCheck
      runQuery_ $ sqlGetIndexes table
      indexes <- fetchMany fetchTableIndex
      runQuery_ $ sqlGetForeignKeys table
      fkeys <- fetchMany fetchForeignKey
      return $ mconcat [
          checkColumns 1 tblColumns desc
        , checkPrimaryKey tblPrimaryKey pk
        , checkChecks tblChecks checks
        , checkIndexes tblIndexes indexes
        , checkForeignKeys tblForeignKeys fkeys
        ]
      where
        fetchTableColumn :: (String, ColumnType, Bool, Maybe String) -> TableColumn
        fetchTableColumn (name, ctype, nullable, mdefault) = TableColumn {
            colName = unsafeSQL name
          , colType = ctype
          , colNullable = nullable
          , colDefault = unsafeSQL `liftM` mdefault
          }

        checkColumns :: Int -> [TableColumn] -> [TableColumn] -> ValidationResult
        checkColumns _ [] [] = mempty
        checkColumns _ rest [] = ValidationResult [tableHasLess "columns" rest]
        checkColumns _ [] rest = ValidationResult [tableHasMore "columns" rest]
        checkColumns !n (d:defs) (c:cols) = mconcat [
            validateNames $ colName d == colName c
          -- bigserial == bigint + autoincrement and there is no
          -- distinction between them after table is created.
          , validateTypes $ colType d == colType c || (colType d == BigSerialT && colType c == BigIntT)
          -- there is a problem with default values determined by sequences as
          -- they're implicitely specified by db, so let's omit them in such case
          , validateDefaults $ colDefault d == colDefault c || (colDefault d == Nothing && ((T.isPrefixOf "nextval('" . unRawSQL) `liftM` colDefault c) == Just True)
          , validateNullables $ colNullable d == colNullable c
          , checkColumns (n+1) defs cols
          ]
          where
            validateNames True = mempty
            validateNames False = ValidationResult [errorMsg ("no. " <> showt n) "names" (unRawSQL . colName)]

            validateTypes True = mempty
            validateTypes False = ValidationResult [errorMsg cname "types" (T.pack . show . colType) <+> sqlHint ("TYPE" <+> columnTypeToSQL (colType d))]

            validateNullables True = mempty
            validateNullables False = ValidationResult [errorMsg cname "nullables" (showt . colNullable) <+> sqlHint ((if colNullable d then "DROP" else "SET") <+> "NOT NULL")]

            validateDefaults True = mempty
            validateDefaults False = ValidationResult [(errorMsg cname "defaults" (showt . fmap unRawSQL . colDefault)) <+> sqlHint set_default]
              where
                set_default = case colDefault d of
                  Just v  -> "SET DEFAULT" <+> v
                  Nothing -> "DROP DEFAULT"

            cname = unRawSQL $ colName d
            errorMsg ident attr f = "Column '" <> ident <> "' differs in" <+> attr <+> "(table:" <+> f c <> ", definition:" <+> f d <> ")."
            sqlHint sql = "(HINT: SQL for making the change is: ALTER TABLE" <+> tblNameText table <+> "ALTER COLUMN" <+> unRawSQL (colName d) <+> unRawSQL sql <> ")"

        checkPrimaryKey :: Maybe PrimaryKey -> Maybe (PrimaryKey, RawSQL ()) -> ValidationResult
        checkPrimaryKey mdef mpk = mconcat [
            checkEquality "PRIMARY KEY" def (map fst pk)
          , checkNames (const (pkName tblName)) pk
          ]
          where
            def = maybeToList mdef
            pk = maybeToList mpk

        checkChecks :: [Check] -> [Check] -> ValidationResult
        checkChecks defs checks = case checkEquality "CHECKs" defs checks of
          ValidationResult [] -> ValidationResult []
          ValidationResult errmsgs -> ValidationResult $ errmsgs ++ [" (HINT: If checks are equal modulo number of parentheses/whitespaces used in conditions, just copy and paste expected output into source code)"]

        checkIndexes :: [TableIndex] -> [(TableIndex, RawSQL ())] -> ValidationResult
        checkIndexes defs indexes = mconcat [
            checkEquality "INDEXes" defs (map fst indexes)
          , checkNames (indexName tblName) indexes
          ]

        checkForeignKeys :: [ForeignKey] -> [(ForeignKey, RawSQL ())] -> ValidationResult
        checkForeignKeys defs fkeys = mconcat [
            checkEquality "FOREIGN KEYs" defs (map fst fkeys)
          , checkNames (fkName tblName) fkeys
          ]

-- | Checks whether database is consistent, performing migrations if
-- necessary. Requires all table names to be in lower case.
--
-- The migrations list must have the following properties:
--   * consecutive 'mgrFrom' numbers
--   * no duplicates
--   * all 'mgrFrom' are less than table version number of the table in
--     the 'tables' list
checkDBConsistency
  :: forall m. (MonadDB m, MonadLog m, MonadThrow m)
  => [MigrateOptions] -> [Domain] -> [Table] -> [Migration m]
  -> m ()
checkDBConsistency options domains tables migrations = do
  -- Check the validity of the migrations list.
  validateMigrationsList

  -- Load version numbers of the tables that actually exist in the DB.
  versions <- mapM checkTableVersion tables
  let tablesWithVersions = zip tables (map (fromMaybe 0) versions)

  if all ((==) 0 . snd) tablesWithVersions

    -- No tables are present, create everything from scratch.
    then do
      createDBSchema
      initializeDB

    -- Migration mode.
    else do
      -- Additional validity checks for the migrations list.
      validateMigrationsListAgainstDB tablesWithVersions
      -- Run migrations, if necessary.
      runMigrations tablesWithVersions

  where
    validateMigrationsList :: m ()
    validateMigrationsList = forM_ tables $ \table -> do
      let presentMigrationVersions
            = map mgrFrom $ filter (\m -> tblName (mgrTable m) == tblName table)
              migrations
          expectedMigrationVersions
            = reverse $ take (length presentMigrationVersions) $
              reverse  [0 .. tblVersion table - 1]
      when (presentMigrationVersions /= expectedMigrationVersions) $ do
        logAttention "Migrations are invalid" $ object [
            "table"                       .= tblNameText table
          , "migration_versions"          .= presentMigrationVersions
          , "expected_migration_versions" .= expectedMigrationVersions
          ]
        error $ "checkDBConsistency: invalid migrations for table"
          <+> tblNameString table

    createDBSchema :: m ()
    createDBSchema = do
      logInfo_ "Creating domains..."
      mapM_ createDomain domains
      -- Create all tables with no constraints first to allow cyclic references.
      logInfo_ "Creating tables..."
      mapM_ (createTable False) tables
      logInfo_ "Creating table constraints..."
      mapM_ createTableConstraints tables
      logInfo_ "Done."

    initializeDB :: m ()
    initializeDB = do
      logInfo_ "Running initial setup for tables..."
      forM_ tables $ \t -> case tblInitialSetup t of
        Nothing -> return ()
        Just tis -> do
          logInfo_ $ "Intializing" <+> tblNameText t <> "..."
          initialSetup tis
      logInfo_ "Done."

    validateMigrationsListAgainstDB :: [(Table, Int32)] -> m ()
    validateMigrationsListAgainstDB tablesWithVersions
      = forM_ tablesWithVersions $ \(table, ver) ->
        when (tblVersion table /= ver) $
        case L.find
          (\m -> tblNameString (mgrTable m) == tblNameString table) migrations of
          Nothing ->
            error $ "checkDBConsistency: no migrations found for table '"
              ++ tblNameString table ++ "', cannot migrate "
              ++ show ver ++ " -> " ++ show (tblVersion table)
          Just m | mgrFrom m > ver ->
            error $ "checkDBConsistency: earliest migration for table '"
              ++ tblNameString table ++ "' is from version "
              ++ show (mgrFrom m) ++ ", cannot migrate "
              ++ show ver ++ " -> " ++ show (tblVersion table)
          Just _ -> return ()

    runMigrations :: [(Table, Int32)] -> m ()
    runMigrations tablesWithVersions = do
      let migrationsToRun
            = filter (\m -> any (\(t, from) -> tblName (mgrTable m) == tblName t
                                  && mgrFrom m >= from) tablesWithVersions)
              migrations

      when (not . null $ migrationsToRun) $ do
        logInfo_ "Running migrations..."
        forM_ migrationsToRun $ \migration -> do
          logInfo_ $ arrListTable (mgrTable migration)
            <> showt (mgrFrom migration) <+> "->"
            <+> showt (succ $ mgrFrom migration)
          mgrDo migration
          runQuery_ $ sqlUpdate "table_versions" $ do
            sqlSet "version" $ succ (mgrFrom migration)
            sqlWhereEq "name" $ tblNameString (mgrTable migration)
          when (ForceCommitAfterEveryMigration `elem` options) $ do
            logInfo_ $ "Forcing commit after migraton"
              <> " and starting new transaction..."
            commit
            begin
            logInfo_ $ "Forcing commit after migraton"
              <> " and starting new transaction... done."
            logInfo_ "!IMPORTANT! Database has been permanently changed"
        logInfo_ "Running migrations... done."

checkTableVersion :: (MonadDB m, MonadThrow m) => Table -> m (Maybe Int32)
checkTableVersion table = do
  doesExist <- runQuery01 . sqlSelect "pg_catalog.pg_class c" $ do
    sqlResult "TRUE"
    sqlLeftJoinOn "pg_catalog.pg_namespace n" "n.oid = c.relnamespace"
    sqlWhereEq "c.relname" $ tblNameString table
    sqlWhere "pg_catalog.pg_table_is_visible(c.oid)"
  if doesExist
    then do
      runQuery_ $ "SELECT version FROM table_versions WHERE name ="
        <?> tblNameString table
      mver <- fetchMaybe runIdentity
      case mver of
        Just ver -> return $ Just ver
        Nothing  -> error $ "checkTableVersion: table '"
          ++ tblNameString table
          ++ "' is present in the database, "
          ++ "but there is no corresponding version info in 'table_versions'."
    else do
      return Nothing

-- *** TABLE STRUCTURE ***

sqlGetTableID :: Table -> SQL
sqlGetTableID table = parenthesize . toSQLCommand $
  sqlSelect "pg_catalog.pg_class c" $ do
    sqlResult "c.oid"
    sqlLeftJoinOn "pg_catalog.pg_namespace n" "n.oid = c.relnamespace"
    sqlWhereEq "c.relname" $ tblNameString table
    sqlWhere "pg_catalog.pg_table_is_visible(c.oid)"

-- *** PRIMARY KEY ***

sqlGetPrimaryKey :: Table -> SQL
sqlGetPrimaryKey table = toSQLCommand . sqlSelect "pg_catalog.pg_constraint c" $ do
  sqlResult "c.conname::text"
  sqlResult "array(SELECT a.attname::text FROM pg_catalog.pg_attribute a WHERE a.attrelid = c.conrelid AND a.attnum = ANY (c.conkey)) as columns" -- list of affected columns
  sqlWhereEq "c.contype" 'p'
  sqlWhereEqSql "c.conrelid" $ sqlGetTableID table

fetchPrimaryKey :: (String, Array1 String) -> Maybe (PrimaryKey, RawSQL ())
fetchPrimaryKey (name, Array1 columns) = (, unsafeSQL name)
  <$> (pkOnColumns $ map unsafeSQL columns)

-- *** CHECKS ***

sqlGetChecks :: Table -> SQL
sqlGetChecks table = toSQLCommand . sqlSelect "pg_catalog.pg_constraint c" $ do
  sqlResult "c.conname::text"
  sqlResult "regexp_replace(pg_get_constraintdef(c.oid, true), 'CHECK \\((.*)\\)', '\\1') AS body" -- check body
  sqlWhereEq "c.contype" 'c'
  sqlWhereEqSql "c.conrelid" $ sqlGetTableID table

fetchTableCheck :: (String, String) -> Check
fetchTableCheck (name, condition) = Check {
  chkName = unsafeSQL name
, chkCondition = unsafeSQL condition
}

-- *** INDEXES ***

sqlGetIndexes :: Table -> SQL
sqlGetIndexes table = toSQLCommand . sqlSelect "pg_catalog.pg_class c" $ do
  sqlResult "c.relname::text" -- index name
  sqlResult $ "ARRAY(" <> selectCoordinates <> ")" -- array of index coordinates
  sqlResult "i.indisunique" -- is it unique?
  sqlResult "pg_catalog.pg_get_expr(i.indpred, i.indrelid, true)" -- if partial, get constraint def
  sqlJoinOn "pg_catalog.pg_index i" "c.oid = i.indexrelid"
  sqlLeftJoinOn "pg_catalog.pg_constraint r" "r.conrelid = i.indrelid AND r.conindid = i.indexrelid"
  sqlWhereEqSql "i.indrelid" $ sqlGetTableID table
  sqlWhereIsNULL "r.contype" -- fetch only "pure" indexes
  where
    -- Get all coordinates of the index.
    selectCoordinates = smconcat [
        "WITH RECURSIVE coordinates(k, name) AS ("
      , "  VALUES (0, NULL)"
      , "  UNION ALL"
      , "    SELECT k+1, pg_catalog.pg_get_indexdef(i.indexrelid, k+1, true)"
      , "      FROM coordinates"
      , "     WHERE pg_catalog.pg_get_indexdef(i.indexrelid, k+1, true) != ''"
      , ")"
      , "SELECT name FROM coordinates WHERE k > 0"
      ]

fetchTableIndex :: (String, Array1 String, Bool, Maybe String) -> (TableIndex, RawSQL ())
fetchTableIndex (name, Array1 columns, unique, mconstraint) = (TableIndex {
  idxColumns = map unsafeSQL columns
, idxUnique = unique
, idxWhere = unsafeSQL `liftM` mconstraint
}, unsafeSQL name)

-- *** FOREIGN KEYS ***

sqlGetForeignKeys :: Table -> SQL
sqlGetForeignKeys table = toSQLCommand . sqlSelect "pg_catalog.pg_constraint r" $ do
  sqlResult "r.conname::text" -- fk name
  sqlResult $ "ARRAY(SELECT a.attname::text FROM pg_catalog.pg_attribute a JOIN (" <> unnestWithOrdinality "r.conkey" <> ") conkeys ON (a.attnum = conkeys.item) WHERE a.attrelid = r.conrelid ORDER BY conkeys.n)" -- constrained columns
  sqlResult "c.relname::text" -- referenced table
  sqlResult $ "ARRAY(SELECT a.attname::text FROM pg_catalog.pg_attribute a JOIN (" <> unnestWithOrdinality "r.confkey" <> ") confkeys ON (a.attnum = confkeys.item) WHERE a.attrelid = r.confrelid ORDER BY confkeys.n)" -- referenced columns
  sqlResult "r.confupdtype" -- on update
  sqlResult "r.confdeltype" -- on delete
  sqlResult "r.condeferrable" -- deferrable?
  sqlResult "r.condeferred" -- initially deferred?
  sqlJoinOn "pg_catalog.pg_class c" "c.oid = r.confrelid"
  sqlWhereEqSql "r.conrelid" $ sqlGetTableID table
  sqlWhereEq "r.contype" 'f'
  where
    unnestWithOrdinality :: RawSQL () -> SQL
    unnestWithOrdinality arr = "SELECT n, " <> raw arr <> "[n] AS item FROM generate_subscripts(" <> raw arr <> ", 1) AS n"

fetchForeignKey :: (String, Array1 String, String, Array1 String, Char, Char, Bool, Bool) -> (ForeignKey, RawSQL ())
fetchForeignKey (name, Array1 columns, reftable, Array1 refcolumns, on_update, on_delete, deferrable, deferred) = (ForeignKey {
  fkColumns = map unsafeSQL columns
, fkRefTable = unsafeSQL reftable
, fkRefColumns = map unsafeSQL refcolumns
, fkOnUpdate = charToForeignKeyAction on_update
, fkOnDelete = charToForeignKeyAction on_delete
, fkDeferrable = deferrable
, fkDeferred = deferred
}, unsafeSQL name)
  where
    charToForeignKeyAction c = case c of
      'a' -> ForeignKeyNoAction
      'r' -> ForeignKeyRestrict
      'c' -> ForeignKeyCascade
      'n' -> ForeignKeySetNull
      'd' -> ForeignKeySetDefault
      _   -> error $ "fetchForeignKey: invalid foreign key action code: " ++ show c

-- *** UTILS ***

tblNameText :: Table -> T.Text
tblNameText = unRawSQL . tblName

tblNameString :: Table -> String
tblNameString = T.unpack . tblNameText

checkEquality :: (Eq t, Show t) => T.Text -> [t] -> [t] -> ValidationResult
checkEquality pname defs props = case (defs L.\\ props, props L.\\ defs) of
  ([], []) -> mempty
  (def_diff, db_diff) -> ValidationResult [mconcat [
      "Table and its definition have diverged and have "
    , showt $ length db_diff
    , " and "
    , showt $ length def_diff
    , " different "
    , pname
    , " each, respectively (table: "
    , T.pack $ show db_diff
    , ", definition: "
    , T.pack $ show def_diff
    , ")."
    ]]

checkNames :: Show t => (t -> RawSQL ()) -> [(t, RawSQL ())] -> ValidationResult
checkNames prop_name = mconcat . map check
  where
    check (prop, name) = case prop_name prop of
      pname
        | pname == name -> mempty
        | otherwise -> ValidationResult [mconcat [
            "Property "
          , T.pack $ show prop
          , " has invalid name (expected: "
          , unRawSQL pname
          , ", given: "
          , unRawSQL name
          , ")."
          ]]

tableHasLess :: Show t => T.Text -> t -> T.Text
tableHasLess ptype missing = "Table in the database has *less*" <+> ptype <+> "than its definition (missing:" <+> T.pack (show missing) <> ")"

tableHasMore :: Show t => T.Text -> t -> T.Text
tableHasMore ptype extra = "Table in the database has *more*" <+> ptype <+> "than its definition (extra:" <+> T.pack (show extra) <> ")"

arrListTable :: Table -> T.Text
arrListTable table = " ->" <+> tblNameText table <> ": "
