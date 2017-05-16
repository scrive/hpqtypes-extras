module Database.PostgreSQL.PQTypes.Checks (
  -- * Checks
    checkDatabase
  , checkDatabaseAllowUnknownTables
  , createTable
  , createDomain

  -- * Migrations
  , MigrateOptions(..)
  , migrateDatabase
  ) where

import Control.Applicative ((<$>))
import Control.Monad.Catch
import Control.Monad.Reader
import Data.Int
import Data.Function (on)
import Data.Maybe
import Data.Monoid
import Data.Monoid.Utils
import Data.Ord (comparing)
import Data.Text (Text)
import Database.PostgreSQL.PQTypes hiding (def)
import Log
import Prelude
import TextShow
import qualified Data.List as L
import qualified Data.Text as T

import Database.PostgreSQL.PQTypes.Checks.Util
import Database.PostgreSQL.PQTypes.Migrate
import Database.PostgreSQL.PQTypes.Model
import Database.PostgreSQL.PQTypes.SQL.Builder
import Database.PostgreSQL.PQTypes.Versions

----------------------------------------

-- | Run migrations and check the database structure.
migrateDatabase
  :: (MonadDB m, MonadLog m, MonadThrow m)
  => [MigrateOptions] -> [Extension] -> [Domain] -> [Table] -> [Migration m]
  -> m ()
migrateDatabase options extensions domains tables migrations = do
  setDBTimeZoneToUTC
  mapM_ checkExtension extensions
  -- 'checkDBConsistency' also performs migrations.
  checkDBConsistency options domains (tableVersions : tables) migrations
  resultCheck =<< checkDomainsStructure domains
  resultCheck =<< checkDBStructure (tableVersions : tables)
  resultCheck =<< checkTablesWereDropped migrations
  resultCheck =<< checkUnknownTables tables
  resultCheck =<< checkExistanceOfVersionsForTables (tableVersions : tables)

  -- everything is OK, commit changes
  commit

-- | Run checks on the database structure and whether the database
-- needs to be migrated. Will do a full check of DB structure
checkDatabase
  :: forall m . (MonadDB m, MonadLog m, MonadThrow m)
  => [Domain] -> [Table] -> m ()
checkDatabase = checkDatabase_ False

-- | Same as `checkDatabase`, but will not failed if there are
-- additional tables in database
checkDatabaseAllowUnknownTables
  :: forall m . (MonadDB m, MonadLog m, MonadThrow m)
  => [Domain] -> [Table] -> m ()
checkDatabaseAllowUnknownTables = checkDatabase_ True

checkDatabase_
  :: forall m . (MonadDB m, MonadLog m, MonadThrow m)
  => Bool -> [Domain] -> [Table] -> m ()
checkDatabase_ allowUnknownTables domains tables = do
  tablesWithVersions <- getTableVersions tables

  resultCheck $ checkVersions tablesWithVersions
  resultCheck =<< checkDomainsStructure domains
  resultCheck =<< checkDBStructure (tableVersions : tables)
  when (not $ allowUnknownTables) $ do
    resultCheck =<< checkUnknownTables tables
    resultCheck =<< checkExistanceOfVersionsForTables (tableVersions : tables)

  -- Check initial setups only after database structure is considered
  -- consistent as before that some of the checks may fail internally.
  resultCheck =<< checkInitialSetups tables

  where
    checkVersions :: [(Table, Int32)] -> ValidationResult
    checkVersions vs = mconcat . map (ValidationResult . checkVersion) $ vs

    checkVersion :: (Table, Int32) -> [Text]
    checkVersion (t@Table{..}, v)
      | tblVersion == v = []
      | v == 0          = ["Table '" <> tblNameText t <> "' must be created"]
      | otherwise       = ["Table '" <> tblNameText t
                           <> "' must be migrated" <+> showt v <+> "->"
                           <+> showt tblVersion]

    checkInitialSetups :: [Table] -> m ValidationResult
    checkInitialSetups tbls =
      liftM mconcat . mapM (liftM ValidationResult . checkInitialSetup') $ tbls

    checkInitialSetup' :: Table -> m [Text]
    checkInitialSetup' t@Table{..} = case tblInitialSetup of
      Nothing -> return []
      Just is -> checkInitialSetup is >>= \case
        True  -> return []
        False -> return ["Initial setup for table '"
                          <> tblNameText t <> "' is not valid"]

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

-- | Check whether the database returns timestamps in UTC, and set the
-- timezone to UTC if it doesn't.
setDBTimeZoneToUTC :: (MonadDB m, MonadLog m, MonadThrow m) => m ()
setDBTimeZoneToUTC = do
  runSQL_ "SHOW timezone"
  timezone :: String <- fetchOne runIdentity
  when (timezone /= "UTC") $ do
    dbname <- currentCatalog
    logInfo_ $ "Setting '" <> unRawSQL dbname
      <> "' database to return timestamps in UTC"
    runQuery_ $ "ALTER DATABASE" <+> dbname <+> "SET TIMEZONE = 'UTC'"

-- | Get the names of all user-defined tables that actually exist in
-- the DB.
getDBTableNames :: (MonadDB m) => m [Text]
getDBTableNames = do
  runQuery_ $ sqlSelect "information_schema.tables" $ do
    sqlResult "table_name::text"
    sqlWhere "table_name <> 'table_versions'"
    sqlWhere "table_type = 'BASE TABLE'"
    sqlWhere "table_schema NOT IN ('information_schema','pg_catalog')"

  dbTableNames <- fetchMany runIdentity
  return dbTableNames

-- | Check that there's a 1-to-1 correspondence between the list of
-- 'Table's and what's actually in the database.
checkUnknownTables :: (MonadDB m, MonadLog m) => [Table] -> m ValidationResult
checkUnknownTables tables = do
  dbTableNames  <- getDBTableNames
  let tableNames = map (unRawSQL . tblName) tables
      absent     = dbTableNames L.\\ tableNames
      notPresent = tableNames   L.\\ dbTableNames

  if (not . null $ absent) || (not . null $ notPresent)
    then do
    mapM_ (logInfo_ . (<+>) "Unknown table:") absent
    mapM_ (logInfo_ . (<+>) "Table not present in the database:") notPresent
    return . ValidationResult $
      [ "Unknown tables:" <+> T.intercalate ", " absent
      , "Tables not present in the database:" <+> T.intercalate ", " notPresent
      ]
    else return mempty

-- | Check that there's a 1-to-1 correspondence between the list of
-- 'Table's and what's actually in the table 'table_versions'.
checkExistanceOfVersionsForTables :: (MonadDB m, MonadLog m) => [Table] -> m ValidationResult
checkExistanceOfVersionsForTables tables = do
  runQuery_ $ sqlSelect "table_versions" $ do
    sqlResult "name::text"
  (existingTableNames :: [Text]) <- fetchMany runIdentity

  let tableNames = map (unRawSQL . tblName) tables
      absent     = existingTableNames L.\\ tableNames
      notPresent = tableNames   L.\\ existingTableNames

  if (not . null $ absent) || (not . null $ notPresent)
    then do
    mapM_ (logInfo_ . (<+>) "Unknown entry in 'table_versions':") absent
    mapM_ (logInfo_ . (<+>) "Table not present in the 'table_versions':") notPresent
    return . ValidationResult $
      [ "Unknown entry in table_versions':" <+> T.intercalate ", " absent
      , "Tables not present in the 'table_versions':" <+> T.intercalate ", " notPresent
      ]
    else return mempty

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
  mdom <- fetchMaybe $ \(dname, dtype, nullable, defval, cnames, conds) ->
    Domain {
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
    Nothing -> ValidationResult ["Domain '" <> unRawSQL (domName def)
                                 <> "' doesn't exist in the database"]
  where
    compareAttr :: (Eq a, Show a)
                => Domain -> Domain -> Text -> (Domain -> a) -> ValidationResult
    compareAttr dom def attrname attr
      | attr dom == attr def = ValidationResult []
      | otherwise = ValidationResult
        [ "Attribute '" <> attrname
          <> "' does not match (database:" <+> T.pack (show $ attr dom)
          <> ", definition:" <+> T.pack (show $ attr def) <> ")" ]

-- | Check that the tables that must have been dropped are actually
-- missing from the DB.
checkTablesWereDropped :: (MonadDB m, MonadThrow m) =>
                          [Migration m] -> m ValidationResult
checkTablesWereDropped mgrs = do
  let droppedTableNames = [ mgrTableName mgr
                          | mgr <- mgrs, isDropTableMigration mgr ]
  fmap mconcat . forM droppedTableNames $
    \tblName -> do
      mver <- checkTableVersion (T.unpack . unRawSQL $ tblName)
      return $ if isNothing mver
               then mempty
               else ValidationResult [ "The table '" <> unRawSQL tblName
                                       <> "' that must have been dropped"
                                       <> " is still present in the database." ]

-- | Checks whether database is consistent.
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
          , validateTypes $ colType d == colType c ||
            (colType d == BigSerialT && colType c == BigIntT)
          -- there is a problem with default values determined by sequences as
          -- they're implicitely specified by db, so let's omit them in such case
          , validateDefaults $ colDefault d == colDefault c ||
            (colDefault d == Nothing
             && ((T.isPrefixOf "nextval('" . unRawSQL) `liftM` colDefault c)
                == Just True)
          , validateNullables $ colNullable d == colNullable c
          , checkColumns (n+1) defs cols
          ]
          where
            validateNames True = mempty
            validateNames False = ValidationResult
              [ errorMsg ("no. " <> showt n) "names" (unRawSQL . colName) ]

            validateTypes True = mempty
            validateTypes False = ValidationResult
              [ errorMsg cname "types" (T.pack . show . colType)
                <+> sqlHint ("TYPE" <+> columnTypeToSQL (colType d)) ]

            validateNullables True = mempty
            validateNullables False = ValidationResult
              [ errorMsg cname "nullables" (showt . colNullable)
                <+> sqlHint ((if colNullable d then "DROP" else "SET")
                              <+> "NOT NULL") ]

            validateDefaults True = mempty
            validateDefaults False = ValidationResult
              [ (errorMsg cname "defaults" (showt . fmap unRawSQL . colDefault))
                <+> sqlHint set_default ]
              where
                set_default = case colDefault d of
                  Just v  -> "SET DEFAULT" <+> v
                  Nothing -> "DROP DEFAULT"

            cname = unRawSQL $ colName d
            errorMsg ident attr f =
              "Column '" <> ident <> "' differs in"
              <+> attr <+> "(table:" <+> f c <> ", definition:" <+> f d <> ")."
            sqlHint sql =
              "(HINT: SQL for making the change is: ALTER TABLE"
              <+> tblNameText table <+> "ALTER COLUMN" <+> unRawSQL (colName d)
              <+> unRawSQL sql <> ")"

        checkPrimaryKey :: Maybe PrimaryKey -> Maybe (PrimaryKey, RawSQL ())
                        -> ValidationResult
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
          ValidationResult errmsgs -> ValidationResult $
            errmsgs ++ [" (HINT: If checks are equal modulo number of parentheses/whitespaces used in conditions, just copy and paste expected output into source code)"]

        checkIndexes :: [TableIndex] -> [(TableIndex, RawSQL ())]
                     -> ValidationResult
        checkIndexes defs indexes = mconcat [
            checkEquality "INDEXes" defs (map fst indexes)
          , checkNames (indexName tblName) indexes
          ]

        checkForeignKeys :: [ForeignKey] -> [(ForeignKey, RawSQL ())]
                         -> ValidationResult
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
  validateMigrations
  validateDropTableMigrations

  -- Load version numbers of the tables that actually exist in the DB.
  tablesWithVersions   <- getTableVersions $ tables
  dbTablesWithVersions <- getDBTableVersions

  if all ((==) 0 . snd) tablesWithVersions

    -- No tables are present, create everything from scratch.
    then do
      createDBSchema
      initializeDB

    -- Migration mode.
    else do
      -- Additional validity checks for the migrations list.
      validateMigrationsAgainstDB [ (tblName table, tblVersion table, actualVer)
                                  | (table, actualVer) <- tablesWithVersions ]
      validateDropTableMigrationsAgainstDB dbTablesWithVersions
      -- Run migrations, if necessary.
      runMigrations dbTablesWithVersions

  where

    errorInvalidMigrations :: [RawSQL ()] -> a
    errorInvalidMigrations tblNames =
      error $ "checkDBConsistency: invalid migrations for tables"
              <+> (L.intercalate ", " $ map (T.unpack . unRawSQL) tblNames)

    checkMigrationsListValidity :: Table -> [Int32] -> [Int32] -> m ()
    checkMigrationsListValidity table presentMigrationVersions
      expectedMigrationVersions = do
      when (presentMigrationVersions /= expectedMigrationVersions) $ do
        logAttention "Migrations are invalid" $ object [
            "table"                       .= tblNameText table
          , "migration_versions"          .= presentMigrationVersions
          , "expected_migration_versions" .= expectedMigrationVersions
          ]
        errorInvalidMigrations [tblName $ table]

    validateMigrations :: m ()
    validateMigrations = forM_ tables $ \table -> do
      let presentMigrationVersions
            = [ mgrFrom | Migration{..} <- migrations
                        , mgrTableName == tblName table ]
          expectedMigrationVersions
            = reverse $ take (length presentMigrationVersions) $
              reverse  [0 .. tblVersion table - 1]
      checkMigrationsListValidity table presentMigrationVersions
        expectedMigrationVersions

    validateDropTableMigrations :: m ()
    validateDropTableMigrations = do
      let droppedTableNames =
            [ mgrTableName $ mgr | mgr <- migrations
                                 , isDropTableMigration mgr ]
          tableNames =
            [ tblName tbl | tbl <- tables ]

      -- Check that the intersection between the 'tables' list and
      -- dropped tables is empty.
      let intersection = L.intersect droppedTableNames tableNames
      when (not . null $ intersection) $ do
          logAttention ("The intersection between tables "
                        <> "and dropped tables is not empty")
            $ object
            [ "intersection" .= map unRawSQL intersection ]
          errorInvalidMigrations [ tblName tbl | tbl <- tables
                                               , tblName tbl `elem` intersection ]

      -- Check that if a list of migrations for a given table has a
      -- drop table migration, it is unique and is the last migration
      -- in the list.
      let migrationsByTable     = L.groupBy ((==) `on` mgrTableName)
                                  migrations
          dropMigrationLists    = [ mgrs | mgrs <- migrationsByTable
                                         , any isDropTableMigration mgrs ]
          invalidMigrationLists =
            [ mgrs | mgrs <- dropMigrationLists
                   , (not . isDropTableMigration . last $ mgrs) ||
                     (length . filter isDropTableMigration $ mgrs) > 1 ]

      when (not . null $ invalidMigrationLists) $ do
        let tablesWithInvalidMigrationLists =
              [ mgrTableName mgr | mgrs <- invalidMigrationLists
                                 , let mgr = head mgrs ]
        logAttention ("Migration lists for some tables contain "
                      <> "either multiple drop table migrations or "
                      <> "a drop table migration in non-tail position.")
          $ object [ "tables" .= [ unRawSQL tblName
                                 | tblName <- tablesWithInvalidMigrationLists ] ]
        errorInvalidMigrations tablesWithInvalidMigrationLists

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
          logInfo_ $ "Initializing" <+> tblNameText t <> "..."
          initialSetup tis
      logInfo_ "Done."

    -- | Input is a list of (table name, expected version, actual version) triples.
    validateMigrationsAgainstDB :: [(RawSQL (), Int32, Int32)] -> m ()
    validateMigrationsAgainstDB tablesWithVersions
      = forM_ tablesWithVersions $ \(tableName, expectedVer, actualVer) ->
        when (expectedVer /= actualVer) $
        case [ m | m@Migration{..} <- migrations
                 , mgrTableName == tableName ] of
          [] ->
            error $ "checkDBConsistency: no migrations found for table '"
              ++ (T.unpack . unRawSQL $ tableName) ++ "', cannot migrate "
              ++ show actualVer ++ " -> " ++ show expectedVer
          (m:_) | mgrFrom m > actualVer ->
                  error $ "checkDBConsistency: earliest migration for table '"
                    ++ (T.unpack . unRawSQL $ tableName) ++ "' is from version "
                    ++ show (mgrFrom m) ++ ", cannot migrate "
                    ++ show actualVer ++ " -> " ++ show expectedVer
                | otherwise -> return ()

    validateDropTableMigrationsAgainstDB :: [(Text, Int32)] -> m ()
    validateDropTableMigrationsAgainstDB dbTablesWithVersions = do
      let dbTablesToDropWithVersions =
            [ (tblName, mgrFrom mgr, fromJust mver)
            | mgr <- migrations
            , isDropTableMigration mgr
            , let tblName = mgrTableName mgr
            , let mver = lookup (unRawSQL tblName) $ dbTablesWithVersions
            , isJust mver ]
      forM_ dbTablesToDropWithVersions $ \(tblName, fromVer, ver) ->
        when (fromVer /= ver) $
          -- In case when the table we're going to drop is an old
          -- version, check that there are migrations that bring it to a new one.
          validateMigrationsAgainstDB [(tblName, fromVer, ver)]

    findMigrationsToRun :: [(Text, Int32)] -> [Migration m]
    findMigrationsToRun dbTablesWithVersions =
      let tableNamesToDrop = [ mgrTableName mgr | mgr <- migrations
                                                , isDropTableMigration mgr ]
          droppedEventually :: Migration m -> Bool
          droppedEventually mgr = mgrTableName mgr `elem` tableNamesToDrop

          lookupVer :: Migration m -> Maybe Int32
          lookupVer mgr = lookup (unRawSQL $ mgrTableName mgr) dbTablesWithVersions

          tableDoesNotExist = isNothing . lookupVer

          -- The idea here is that we find the first migration we need
          -- to run and then just run all migrations in order after
          -- that one.
          migrationsToRun' = dropWhile
            (\mgr ->
               case lookupVer mgr of
                 -- Table doesn't exist in the DB. If it's a create
                 -- table migration and we're not going to drop the
                 -- table afterwards, this is our starting point.
                 Nothing -> not $
                            (mgrFrom mgr == 0) && (not . droppedEventually $ mgr)
                 -- Table exists in the DB. Run only those migrations
                 -- that have mgrFrom >= table version in the DB.
                 Just ver -> not $
                             mgrFrom mgr >= ver)
            migrations

          -- Special case: also include migrations for tables that do
          -- not exist in the DB and ARE going to be dropped if they
          -- come as a consecutive list before the starting point that
          -- we've found.
          --
          -- Case in point: createTable t, doSomethingTo t,
          -- doSomethingTo t1, dropTable t.
          l = length migrationsToRun'
          initialMigrations = drop l $ reverse migrations
          additionalMigrations = takeWhile
            (\mgr -> droppedEventually mgr && tableDoesNotExist mgr)
            initialMigrations
          migrationsToRun = (reverse additionalMigrations) ++ migrationsToRun'
      in migrationsToRun

    runMigration :: (Migration m) -> m ()
    runMigration Migration{..} = do
      case mgrAction of
        StandardMigration mgrDo -> do
          logInfo_ $ arrListTable mgrTableName <> showt mgrFrom <+> "->"
            <+> showt (succ mgrFrom)
          mgrDo
          runQuery_ $ sqlUpdate "table_versions" $ do
            sqlSet "version"  (succ mgrFrom)
            sqlWhereEq "name" (T.unpack . unRawSQL $ mgrTableName)

        DropTableMigration mgrDropTableMode -> do
          logInfo_ $ arrListTable mgrTableName <> "drop table"
          runQuery_ $ sqlDropTable mgrTableName
            mgrDropTableMode
          runQuery_ $ sqlDelete "table_versions" $ do
            sqlWhereEq "name" (T.unpack . unRawSQL $ mgrTableName)

    runMigrations :: [(Text, Int32)] -> m ()
    runMigrations dbTablesWithVersions = do
      let migrationsToRun = findMigrationsToRun dbTablesWithVersions
      validateMigrationsToRun migrationsToRun dbTablesWithVersions
      when (not . null $ migrationsToRun) $ do
        logInfo_ "Running migrations..."
        forM_ migrationsToRun $ \mgr -> do
          runMigration mgr

          when (ForceCommitAfterEveryMigration `elem` options) $ do
            logInfo_ $ "Forcing commit after migraton"
              <> " and starting new transaction..."
            commit
            begin
            logInfo_ $ "Forcing commit after migraton"
              <> " and starting new transaction... done."
            logInfo_ "!IMPORTANT! Database has been permanently changed"
        logInfo_ "Running migrations... done."

    validateMigrationsToRun :: [Migration m] -> [(Text, Int32)] -> m ()
    validateMigrationsToRun migrationsToRun dbTablesWithVersions = do
      let migrationsToRunGrouped =
            L.groupBy ((==) `on` mgrTableName) .
            L.sortBy (comparing mgrTableName) $ -- NB: stable sort
            migrationsToRun
          mgrGroupsNotInDB =
            [ mgrGroup
            | mgrGroup <- migrationsToRunGrouped
            , isNothing $
              lookup (unRawSQL . mgrTableName . head $ mgrGroup)
              dbTablesWithVersions
            ]
          groupsStartingWithDropTable =
            [ mgrGroup
            | mgrGroup <- mgrGroupsNotInDB
            , isDropTableMigration $ head mgrGroup
            ]
          groupsNotStartingWithCreateTable =
            [ mgrGroup
            | mgrGroup <- mgrGroupsNotInDB
            , mgrFrom (head mgrGroup) /= 0
            ]
          tblNames grps =
            [ mgrTableName . head $ grp | grp <- grps ]

      when (not . null $ groupsStartingWithDropTable) $ do
        let tnms = tblNames groupsStartingWithDropTable
        logAttention "There are drop table migrations for non-existing tables."
          $ object [ "tables" .= [ unRawSQL tn | tn <- tnms ] ]
        errorInvalidMigrations tnms

      -- NB: the following check can break if we allow renaming tables.
      when (not . null $ groupsNotStartingWithCreateTable) $ do
        let tnms = tblNames groupsNotStartingWithCreateTable
        logAttention
          ("Some tables haven't been created yet, but" <>
            "their migration lists don't start with a create table migration.")
          $ object [ "tables" .=  [ unRawSQL tn | tn <- tnms ] ]
        errorInvalidMigrations tnms


-- | Associate each table in the list with its version as it exists in
-- the DB, or 0 if it's missing from the DB.
getTableVersions :: (MonadDB m, MonadThrow m) => [Table] -> m [(Table, Int32)]
getTableVersions tbls =
  sequence
  [ (\mver -> (tbl, fromMaybe 0 mver)) <$> checkTableVersion (tblNameString tbl)
  | tbl <- tbls ]

-- | Like 'getTableVersions', but for all user-defined tables that
-- actually exist in the DB.
getDBTableVersions :: (MonadDB m, MonadThrow m) => m [(Text, Int32)]
getDBTableVersions = do
  dbTableNames <- getDBTableNames
  sequence
    [ (\mver -> (name, fromMaybe 0 mver)) <$> checkTableVersion (T.unpack name)
    | name <- dbTableNames ]

-- | Check whether the table exists in the DB, and return 'Just' its
-- version if it does, or 'Nothing' if it doesn't.
checkTableVersion :: (MonadDB m, MonadThrow m) => String -> m (Maybe Int32)
checkTableVersion tblName = do
  doesExist <- runQuery01 . sqlSelect "pg_catalog.pg_class c" $ do
    sqlResult "TRUE"
    sqlLeftJoinOn "pg_catalog.pg_namespace n" "n.oid = c.relnamespace"
    sqlWhereEq "c.relname" $ tblName
    sqlWhere "pg_catalog.pg_table_is_visible(c.oid)"
  if doesExist
    then do
      runQuery_ $ "SELECT version FROM table_versions WHERE name ="
        <?> tblName
      mver <- fetchMaybe runIdentity
      case mver of
        Just ver -> return $ Just ver
        Nothing  -> error $ "checkTableVersion: table '"
          ++ tblName
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
  -- list of affected columns
  sqlResult "array(SELECT a.attname::text FROM pg_catalog.pg_attribute a WHERE a.attrelid = c.conrelid AND a.attnum = ANY (c.conkey)) as columns"
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
  -- if partial, get constraint def
  sqlResult "pg_catalog.pg_get_expr(i.indpred, i.indrelid, true)"
  sqlJoinOn "pg_catalog.pg_index i" "c.oid = i.indexrelid"
  sqlLeftJoinOn "pg_catalog.pg_constraint r"
    "r.conrelid = i.indrelid AND r.conindid = i.indexrelid"
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

fetchTableIndex :: (String, Array1 String, Bool, Maybe String)
                -> (TableIndex, RawSQL ())
fetchTableIndex (name, Array1 columns, unique, mconstraint) = (TableIndex {
  idxColumns = map unsafeSQL columns
, idxUnique = unique
, idxWhere = unsafeSQL `liftM` mconstraint
}, unsafeSQL name)

-- *** FOREIGN KEYS ***

sqlGetForeignKeys :: Table -> SQL
sqlGetForeignKeys table = toSQLCommand
                          . sqlSelect "pg_catalog.pg_constraint r" $ do
  sqlResult "r.conname::text" -- fk name
  sqlResult $
    "ARRAY(SELECT a.attname::text FROM pg_catalog.pg_attribute a JOIN ("
    <> unnestWithOrdinality "r.conkey"
    <> ") conkeys ON (a.attnum = conkeys.item) WHERE a.attrelid = r.conrelid ORDER BY conkeys.n)" -- constrained columns
  sqlResult "c.relname::text" -- referenced table
  sqlResult $ "ARRAY(SELECT a.attname::text FROM pg_catalog.pg_attribute a JOIN ("
    <> unnestWithOrdinality "r.confkey"
    <> ") confkeys ON (a.attnum = confkeys.item) WHERE a.attrelid = r.confrelid ORDER BY confkeys.n)" -- referenced columns
  sqlResult "r.confupdtype" -- on update
  sqlResult "r.confdeltype" -- on delete
  sqlResult "r.condeferrable" -- deferrable?
  sqlResult "r.condeferred" -- initially deferred?
  sqlJoinOn "pg_catalog.pg_class c" "c.oid = r.confrelid"
  sqlWhereEqSql "r.conrelid" $ sqlGetTableID table
  sqlWhereEq "r.contype" 'f'
  where
    unnestWithOrdinality :: RawSQL () -> SQL
    unnestWithOrdinality arr =
      "SELECT n, " <> raw arr
      <> "[n] AS item FROM generate_subscripts(" <> raw arr <> ", 1) AS n"

fetchForeignKey ::
  (String, Array1 String, String, Array1 String, Char, Char, Bool, Bool)
  -> (ForeignKey, RawSQL ())
fetchForeignKey
  ( name, Array1 columns, reftable, Array1 refcolumns
  , on_update, on_delete, deferrable, deferred ) = (ForeignKey {
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
