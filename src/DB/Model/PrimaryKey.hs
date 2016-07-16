module DB.Model.PrimaryKey (
    PrimaryKey
  , pkOnColumn
  , pkOnColumns
  , pkName
  , sqlAddPK
  , sqlDropPK
  ) where

import Data.Monoid.Utils
import Database.PostgreSQL.PQTypes
import Prelude
import qualified Data.Set as S

newtype PrimaryKey = PrimaryKey (S.Set (RawSQL ()))
  deriving (Eq, Ord, Show)

pkOnColumn :: RawSQL () -> Maybe PrimaryKey
pkOnColumn = Just . PrimaryKey . S.singleton

pkOnColumns :: [RawSQL ()] -> Maybe PrimaryKey
pkOnColumns [] = Nothing
pkOnColumns columns = Just . PrimaryKey . S.fromList $ columns

pkName :: RawSQL () -> RawSQL ()
pkName tname = mconcat ["pk__", tname]

sqlAddPK :: RawSQL () -> PrimaryKey -> RawSQL ()
sqlAddPK tname (PrimaryKey columns) = smconcat [
    "ADD CONSTRAINT"
  , pkName tname
  , "PRIMARY KEY ("
  , mintercalate ", " $ S.toAscList columns
  , ")"
  ]

sqlDropPK :: RawSQL () -> RawSQL ()
sqlDropPK tname = "DROP CONSTRAINT" <+> pkName tname
