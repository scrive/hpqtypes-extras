module DB.Model.ForeignKey (
    ForeignKey(..)
  , ForeignKeyAction(..)
  , fkOnColumn
  , fkOnColumns
  , fkName
  , sqlAddFK
  , sqlDropFK
  ) where

import Data.Monoid
import Data.Monoid.Utils
import Database.PostgreSQL.PQTypes
import Prelude
import qualified Data.Text as T

data ForeignKey = ForeignKey {
  fkColumns    :: [RawSQL ()]
, fkRefTable   :: RawSQL ()
, fkRefColumns :: [RawSQL ()]
, fkOnUpdate   :: ForeignKeyAction
, fkOnDelete   :: ForeignKeyAction
, fkDeferrable :: Bool
, fkDeferred   :: Bool
} deriving (Eq, Ord, Show)

data ForeignKeyAction
  = ForeignKeyNoAction
  | ForeignKeyRestrict
  | ForeignKeyCascade
  | ForeignKeySetNull
  | ForeignKeySetDefault
  deriving (Eq, Ord, Show)

fkOnColumn :: RawSQL () -> RawSQL () -> RawSQL () -> ForeignKey
fkOnColumn column reftable refcolumn =
  fkOnColumns [column] reftable [refcolumn]

fkOnColumns :: [RawSQL ()] -> RawSQL () -> [RawSQL ()] -> ForeignKey
fkOnColumns columns reftable refcolumns = ForeignKey {
  fkColumns    = columns
, fkRefTable   = reftable
, fkRefColumns = refcolumns
, fkOnUpdate   = ForeignKeyCascade
, fkOnDelete   = ForeignKeyNoAction
, fkDeferrable = True
, fkDeferred   = False
}

fkName :: RawSQL () -> ForeignKey -> RawSQL ()
fkName tname ForeignKey{..} = shorten $ mconcat [
    "fk__"
  , tname
  , "__"
  , mintercalate "__" fkColumns
  , "__"
  , fkRefTable
  ]
  where
    -- PostgreSQL's limit for identifier is 63 characters
    shorten = flip rawSQL () . T.take 63 . unRawSQL

sqlAddFK :: RawSQL () -> ForeignKey -> RawSQL ()
sqlAddFK tname fk@ForeignKey{..} = mconcat [
    "ADD CONSTRAINT" <+> fkName tname fk <+> "FOREIGN KEY ("
  , mintercalate ", " fkColumns
  , ") REFERENCES" <+> fkRefTable <+> "("
  , mintercalate ", " fkRefColumns
  , ") ON UPDATE" <+> foreignKeyActionToSQL fkOnUpdate
  , "  ON DELETE" <+> foreignKeyActionToSQL fkOnDelete
  , " " <> if fkDeferrable then "DEFERRABLE" else "NOT DEFERRABLE"
  , " INITIALLY" <+> if fkDeferred then "DEFERRED" else "IMMEDIATE"
  ]
  where
    foreignKeyActionToSQL ForeignKeyNoAction = "NO ACTION"
    foreignKeyActionToSQL ForeignKeyRestrict = "RESTRICT"
    foreignKeyActionToSQL ForeignKeyCascade = "CASCADE"
    foreignKeyActionToSQL ForeignKeySetNull = "SET NULL"
    foreignKeyActionToSQL ForeignKeySetDefault = "SET DEFAULT"

sqlDropFK :: RawSQL () -> ForeignKey -> RawSQL ()
sqlDropFK tname fk = "DROP CONSTRAINT" <+> fkName tname fk
