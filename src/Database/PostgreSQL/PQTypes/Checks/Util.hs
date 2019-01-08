{-# LANGUAGE CPP #-}
module Database.PostgreSQL.PQTypes.Checks.Util (
  ValidationResult,
  validationError,
  validationInfo,
  mapValidationResult,
  validationErrorsToInfos,
  resultCheck,
  topMessage,
  tblNameText,
  tblNameString,
  checkEquality,
  checkNames,
  checkPKPresence,
  tableHasLess,
  tableHasMore,
  arrListTable
  ) where

import Control.Monad.Catch
#if !MIN_VERSION_base(4,11,0)
import Data.Monoid
#endif
import Data.Monoid.Utils
import Data.Text (Text)
import Log
import TextShow
import qualified Data.List as L
import qualified Data.Text as T
import qualified Data.Semigroup as SG

import Database.PostgreSQL.PQTypes.Model
import Database.PostgreSQL.PQTypes

-- | A (potentially empty) list of info/error messages.
data ValidationResult = ValidationResult
  { vrInfos  :: [Text]
  , vrErrors :: [Text]
  }

validationError :: Text -> ValidationResult
validationError err = mempty { vrErrors = [err] }

validationInfo :: Text -> ValidationResult
validationInfo msg  = mempty { vrInfos = [msg] }

-- | Downgrade all error messages in a ValidationResult to info messages.
validationErrorsToInfos :: ValidationResult -> ValidationResult
validationErrorsToInfos ValidationResult{..} =
  mempty { vrInfos = vrInfos <> vrErrors }

mapValidationResult ::
  ([Text] -> [Text]) -> ([Text] -> [Text]) -> ValidationResult -> ValidationResult
mapValidationResult mapInfos mapErrs ValidationResult{..} =
  mempty { vrInfos = mapInfos vrInfos, vrErrors = mapErrs vrErrors }

instance SG.Semigroup ValidationResult where
  (ValidationResult infos0 errs0) <> (ValidationResult infos1 errs1)
    = ValidationResult (infos0 <> infos1) (errs0 <> errs1)

instance Monoid ValidationResult where
  mempty  = ValidationResult [] []
  mappend = (SG.<>)

topMessage :: Text -> Text -> ValidationResult -> ValidationResult
topMessage objtype objname vr@ValidationResult{..} =
  case vrErrors of
    [] -> vr
    es -> ValidationResult vrInfos
          ("There are problems with the" <+>
            objtype <+> "'" <> objname <> "'" : es)

-- | Log all messages in a 'ValidationResult', and fail if any of them
-- were errors.
resultCheck
  :: (MonadLog m, MonadThrow m)
  => ValidationResult
  -> m ()
resultCheck ValidationResult{..} = do
  mapM_ logInfo_ vrInfos
  case vrErrors of
    []   -> return ()
    msgs -> do
      mapM_ logAttention_ msgs
      error "resultCheck: validation failed"

----------------------------------------

tblNameText :: Table -> Text
tblNameText = unRawSQL . tblName

tblNameString :: Table -> String
tblNameString = T.unpack . tblNameText

checkEquality :: (Eq t, Show t) => Text -> [t] -> [t] -> ValidationResult
checkEquality pname defs props = case (defs L.\\ props, props L.\\ defs) of
  ([], []) -> mempty
  (def_diff, db_diff) -> validationError . mconcat $ [
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
    ]

checkNames :: Show t => (t -> RawSQL ()) -> [(t, RawSQL ())] -> ValidationResult
checkNames prop_name = mconcat . map check
  where
    check (prop, name) = case prop_name prop of
      pname
        | pname == name -> mempty
        | otherwise     -> validationError . mconcat $ [
            "Property "
          , T.pack $ show prop
          , " has invalid name (expected: "
          , unRawSQL pname
          , ", given: "
          , unRawSQL name
          , ")."
          ]

-- | Check presence of primary key on the named table. We cover all the cases so
-- this could be used standalone, but note that the those where the table source
-- definition and the table in the database differ in this respect is also
-- covered by @checkEquality@.
checkPKPresence :: RawSQL ()
                -- ^ The name of the table to check for presence of primary key
              -> Maybe PrimaryKey
                -- ^ A possible primary key gotten from the table data structure
              -> Maybe (PrimaryKey, RawSQL ())
                -- ^ A possible primary key as retrieved from database along
                -- with its name
              -> ValidationResult
checkPKPresence tableName mdef mpk =
  case (mdef, mpk) of
    (Nothing, Nothing) -> valRes [noSrc, noTbl]
    (Nothing, Just _)  -> valRes [noSrc]
    (Just _, Nothing)  -> valRes [noTbl]
    _                  -> mempty
  where
    noSrc = "no source definition"
    noTbl = "no table definition"
    valRes msgs =
        validationError . mconcat $
        [ "Table ", unRawSQL tableName
        , " has no primary key defined "
        , " (" <> (mintercalate ", " msgs) <> ")"]


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
