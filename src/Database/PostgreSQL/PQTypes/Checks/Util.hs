module Database.PostgreSQL.PQTypes.Checks.Util (
  ValidationResult(..),
  resultCheck,
  topMessage,
  tblNameText,
  tblNameString,
  checkEquality,
  checkNames,
  checkPresence,
  tableHasLess,
  tableHasMore,
  arrListTable
  ) where

import Control.Monad.Catch
import Data.Monoid
import Data.Monoid.Utils
import Data.Text (Text)
import Log
import TextShow
import qualified Data.List as L
import qualified Data.Text as T

import Database.PostgreSQL.PQTypes.Model
import Database.PostgreSQL.PQTypes

-- | A (potentially empty) list of error messages.
newtype ValidationResult = ValidationResult [Text]

instance Monoid ValidationResult where
  mempty = ValidationResult []
  mappend (ValidationResult a) (ValidationResult b) = ValidationResult (a ++ b)

topMessage :: Text -> Text -> ValidationResult -> ValidationResult
topMessage objtype objname = \case
  ValidationResult [] -> ValidationResult []
  ValidationResult es -> ValidationResult
    ("There are problems with the" <+> objtype <+> "'" <> objname <> "'" : es)

resultCheck
  :: (MonadLog m, MonadThrow m)
  => ValidationResult
  -> m ()
resultCheck = \case
  ValidationResult [] -> return ()
  ValidationResult msgs -> do
    mapM_ logAttention_ msgs
    error "resultCheck: validation failed"

tblNameText :: Table -> Text
tblNameText = unRawSQL . tblName

tblNameString :: Table -> String
tblNameString = T.unpack . tblNameText

checkEquality :: (Eq t, Show t) => Text -> [t] -> [t] -> ValidationResult
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

checkPresence :: RawSQL ()
              -> Maybe PrimaryKey
              -> Maybe (PrimaryKey, RawSQL ())
              -> ValidationResult
checkPresence tableName mdef mpk =
  case (mdef, mpk) of
    (Nothing, Nothing) -> valRes [noSrc, noTbl]
    (Nothing, Just _)  -> valRes [noSrc]
    (Just _, Nothing)  -> valRes [noTbl]
    _                  -> mempty
  where
    noSrc = "no source definition"
    noTbl = "no table definition"
    valRes msgs =
        ValidationResult [
          mconcat [ "Table ", unRawSQL tableName
                  , " has no primary key defined "
                  , " (" <> (mintercalate ", " msgs) <> ")"]]


tableHasLess :: Show t => Text -> t -> Text
tableHasLess ptype missing =
  "Table in the database has *less*" <+> ptype <+>
  "than its definition (missing:" <+> T.pack (show missing) <> ")"

tableHasMore :: Show t => Text -> t -> Text
tableHasMore ptype extra =
  "Table in the database has *more*" <+> ptype <+>
  "than its definition (extra:" <+> T.pack (show extra) <> ")"

arrListTable :: RawSQL () -> Text
arrListTable tableName = " ->" <+> unRawSQL tableName <> ": "
