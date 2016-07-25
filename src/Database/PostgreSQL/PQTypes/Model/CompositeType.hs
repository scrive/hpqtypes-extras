module Database.PostgreSQL.PQTypes.Model.CompositeType (
    CompositeType(..)
  , CompositeColumn(..)
  , defineComposites
  ) where

import Data.Monoid.Utils
import Database.PostgreSQL.PQTypes
import Prelude

import Database.PostgreSQL.PQTypes.Model.ColumnType

data CompositeType = CompositeType {
  ctName    :: !(RawSQL ())
, ctColumns :: ![CompositeColumn]
} deriving (Eq, Ord, Show)

data CompositeColumn = CompositeColumn {
  ccName :: !(RawSQL ())
, ccType :: ColumnType
} deriving (Eq, Ord, Show)

-- | Composite types are static in a sense that they can either
-- be created or dropped, altering them is not possible. Therefore
-- they are not part of the migration process. This is not a problem
-- since their exclusive usage is for intermediate representation
-- of complex nested data structures fetched from the database.
defineComposites :: MonadDB m => [CompositeType] -> m ()
defineComposites ctypes = do
  mapM_ (runQuery_ . sqlDropComposite)   $ reverse ctypes
  mapM_ (runQuery_ . sqlCreateComposite) $ ctypes
  where
    sqlCreateComposite CompositeType{..} = smconcat [
        "CREATE TYPE"
      , ctName
      , "AS ("
      , mintercalate ", " $ map columnToSQL ctColumns
      , ")"
      ]
      where
        columnToSQL CompositeColumn{..} = ccName <+> columnTypeToSQL ccType

    sqlDropComposite = ("DROP TYPE IF EXISTS" <+>) . ctName
