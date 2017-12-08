module Database.PostgreSQL.PQTypes.Model.PrimaryKey (
    PrimaryKey
  , pkOnColumn
  , pkOnColumns
  , pkName
  , sqlAddPK
  , sqlDropPK
  ) where

import Data.Monoid (mconcat)
import Data.Monoid.Utils
import Database.PostgreSQL.PQTypes
import Prelude
import Database.PostgreSQL.PQTypes.Utils.NubList

newtype PrimaryKey = PrimaryKey (NubList (RawSQL ()))
  deriving (Eq, Show)

pkOnColumn :: RawSQL () -> Maybe PrimaryKey
pkOnColumn column = Just . PrimaryKey . toNubList $ [column]

pkOnColumns :: [RawSQL ()] -> Maybe PrimaryKey
pkOnColumns []      = Nothing
pkOnColumns columns = Just . PrimaryKey . toNubList $ columns

pkName :: RawSQL () -> RawSQL ()
pkName tname = mconcat ["pk__", tname]

sqlAddPK :: RawSQL () -> PrimaryKey -> RawSQL ()
sqlAddPK tname (PrimaryKey columns) = smconcat [
    "ADD CONSTRAINT"
  , pkName tname
  , "PRIMARY KEY ("
  , mintercalate ", " $ fromNubList columns
  , ")"
  ]

sqlDropPK :: RawSQL () -> RawSQL ()
sqlDropPK tname = "DROP CONSTRAINT" <+> pkName tname
