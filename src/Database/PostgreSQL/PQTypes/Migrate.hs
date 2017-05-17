module Database.PostgreSQL.PQTypes.Migrate (
  MigrateOptions(..),
  createDomain,
  createTable,
  createTableConstraints
  ) where

import Control.Monad
import qualified Data.Foldable as F

import Database.PostgreSQL.PQTypes hiding (def)
import Database.PostgreSQL.PQTypes.Checks.Util
import Database.PostgreSQL.PQTypes.Model
import Database.PostgreSQL.PQTypes.SQL.Builder

data MigrateOptions = ForceCommitAfterEveryMigration
  deriving Eq

createDomain :: MonadDB m => Domain -> m ()
createDomain dom@Domain{..} = do
  -- create the domain
  runQuery_ $ sqlCreateDomain dom
  -- add constraint checks to the domain
  F.forM_ domChecks $ runQuery_ . sqlAlterDomain domName . sqlAddCheck

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
