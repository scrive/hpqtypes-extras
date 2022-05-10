-- |
-- Module: Database.PostgreSQL.PQTypes.Model.Trigger
--
-- Trigger name must be unique among triggers of same table. Only @CONSTRAINT@ triggers are
-- supported. They can only be run @AFTER@ an event. The associated functions are always
-- created with no arguments and always @RETURN TRIGGER@.
--
-- For details, see <https://www.postgresql.org/docs/11/sql-createtrigger.html>.

module Database.PostgreSQL.PQTypes.Model.Trigger (
  -- * Trigger functions
    TriggerFunction(..)
  , sqlCreateTriggerFunction
  , sqlDropTriggerFunction
  -- * Triggers
  , TriggerEvent(..)
  , Trigger(..)
  , triggerMakeName
  , triggerBaseName
  , sqlCreateTrigger
  , sqlDropTrigger
  , createTrigger
  , dropTrigger
  , getDBTriggers
  ) where

import Data.Bits (testBit)
import Data.Foldable (foldl')
import Data.Int
import Data.Monoid.Utils
import Data.Set (Set)
import Data.Text (Text)
import Database.PostgreSQL.PQTypes
import Database.PostgreSQL.PQTypes.SQL.Builder
import qualified Data.Set as Set
import qualified Data.Text as Text

-- | Function associated with a trigger.
--
-- @since 1.15.0.0
data TriggerFunction = TriggerFunction {
    tfName   :: RawSQL ()
    -- ^ The function's name.
  , tfSource :: RawSQL ()
    -- ^ The functions's body source code.
} deriving (Show)

instance Eq TriggerFunction where
  -- Since the functions have no arguments, it's impossible to create two functions with
  -- the same name. Therefore comparing functions only by their names is enough in this
  -- case. The assumption is, of course, that the database schema is only changed using
  -- this framework.
  f1 == f2 = tfName f1 == tfName f2

-- | Build an SQL statement for creating a trigger function.
--
-- Since we only support @CONSTRAINT@ triggers, the function will always @RETURN TRIGGER@
-- and will have no parameters.
--
-- @since 1.15.0.0
sqlCreateTriggerFunction :: TriggerFunction -> RawSQL ()
sqlCreateTriggerFunction TriggerFunction{..} =
  "CREATE FUNCTION"
    <+> tfName
    <>  "()"
    <+> "RETURNS TRIGGER"
    <+> "AS $$"
    <+> tfSource
    <+> "$$"
    <+> "LANGUAGE PLPGSQL"
    <+> "VOLATILE"
    <+> "RETURNS NULL ON NULL INPUT"

-- | Build an SQL statement for dropping a trigger function.
--
-- @since 1.15.0.0
sqlDropTriggerFunction :: TriggerFunction -> RawSQL ()
sqlDropTriggerFunction TriggerFunction{..} =
  "DROP FUNCTION" <+> tfName <+> "RESTRICT"

-- | Trigger event name.
--
-- @since 1.15.0.0
data TriggerEvent
  = TriggerInsert
  -- ^ The @INSERT@ event.
  | TriggerUpdate
  -- ^ The @UPDATE@ event.
  | TriggerUpdateOf [RawSQL ()]
  -- ^ The @UPDATE OF column1 [, column2 ...]@ event.
  | TriggerDelete
  -- ^ The @DELETE@ event.
  deriving (Eq, Ord, Show)

-- | Trigger.
--
-- @since 1.15.0.0
data Trigger = Trigger {
    triggerTable             :: RawSQL ()
    -- ^ The table that the trigger is associated with.
  , triggerName              :: RawSQL ()
    -- ^ The internal name without any prefixes. Trigger name must be unique among
    -- triggers of same table. See 'triggerMakeName'.
  , triggerEvents            :: Set TriggerEvent
    -- ^ The set of events. Corresponds to the @{ __event__ [ OR ... ] }@ in the trigger
    -- definition. The order in which they are defined doesn't matter and there can
    -- only be one of each.
  , triggerDeferrable        :: Bool
    -- ^ Is the trigger @DEFERRABLE@ or @NOT DEFERRABLE@ ?
  , triggerInitiallyDeferred :: Bool
    -- ^ Is the trigger @INITIALLY DEFERRED@ or @INITIALLY IMMEDIATE@ ?
  , triggerWhen              :: Maybe (RawSQL ())
    -- ^ The condition that specifies whether the trigger should fire. Corresponds to the
    -- @WHEN ( __condition__ )@ in the trigger definition.
  , triggerFunction          :: TriggerFunction
    -- ^ The function to execute when the trigger fires.
} deriving (Eq, Show)

-- | Make a trigger name that can be used in SQL.
--
-- Given a base @name@ and @tableName@, return a new name that will be used as the
-- actual name of the trigger in an SQL query. The returned name is in the format
-- @trg\__\<tableName\>\__\<name\>@.
--
-- @since 1.15.0
triggerMakeName :: RawSQL () -> RawSQL () -> RawSQL ()
triggerMakeName name tableName = "trg__" <> tableName <> "__" <> name

-- | Return the trigger's base name.
--
-- Given the trigger's actual @name@ and @tableName@, return the base name of the
-- trigger. This is basically the reverse of what 'triggerMakeName' does.
--
-- @since 1.15.0
triggerBaseName :: RawSQL () -> RawSQL () -> RawSQL ()
triggerBaseName name tableName =
  rawSQL (snd . Text.breakOnEnd (unRawSQL tableName <> "__") $ unRawSQL name) ()

triggerEventName :: TriggerEvent -> RawSQL ()
triggerEventName = \case
  TriggerInsert -> "INSERT"
  TriggerUpdate -> "UPDATE"
  TriggerUpdateOf columns -> if null columns
                             then error "UPDATE OF must have columns."
                             else "UPDATE OF" <+> mintercalate ", " columns
  TriggerDelete -> "DELETE"

-- | Build an SQL statement that creates a trigger.
--
-- Only supports @CONSTRAINT@ triggers which can only run @AFTER@.
--
-- @since 1.15.0
sqlCreateTrigger :: Trigger -> RawSQL ()
sqlCreateTrigger Trigger{..} =
  "CREATE CONSTRAINT TRIGGER" <+> trgName
    <+> "AFTER" <+> trgEvents
    <+> "ON" <+> triggerTable
    <+> trgTiming
    <+> "FOR EACH ROW"
    <+> trgWhen
    <+> "EXECUTE FUNCTION" <+> trgFunction
    <+> "()"
  where
    trgName
      | triggerName == "" = error "Trigger must have a name."
      | otherwise = triggerMakeName triggerName triggerTable
    trgEvents
      | triggerEvents == Set.empty = error "Trigger must have at least one event."
      | otherwise = mintercalate " OR " . map triggerEventName $ Set.toList triggerEvents
    trgTiming = let deferrable = (if triggerDeferrable then "" else "NOT") <+> "DEFERRABLE"
                    deferred   = if triggerInitiallyDeferred
                                 then "INITIALLY DEFERRED"
                                 else "INITIALLY IMMEDIATE"
                in deferrable <+> deferred
    trgWhen = maybe "" (\w -> "WHEN (" <+> w <+> ")") triggerWhen
    trgFunction = tfName triggerFunction


-- | Build an SQL statement that drops a trigger.
--
-- @since 1.15.0
sqlDropTrigger :: Trigger -> RawSQL ()
sqlDropTrigger Trigger{..} =
  -- In theory, because the trigger is dependent on its function, it should be enough to
  -- 'DROP FUNCTION triggerFunction CASCADE'. However, let's make this safe and go with
  -- the default RESTRICT here.
  "DROP TRIGGER" <+> trgName <+> "ON" <+> triggerTable <+> "RESTRICT"
  where
    trgName
      | triggerName == "" = error "Trigger must have a name."
      | otherwise = triggerMakeName triggerName triggerTable

-- | Create the trigger in the database.
--
-- First, create the trigger's associated function, then create the trigger itself.
--
-- @since 1.15.0
createTrigger :: MonadDB m => Trigger -> m ()
createTrigger trigger = do
  -- TODO: Use 'withTransaction' here? That would mean adding MonadMask...
  runQuery_ . sqlCreateTriggerFunction $ triggerFunction trigger
  runQuery_ $ sqlCreateTrigger trigger

-- | Drop the trigger from the database.
--
-- @since 1.15.0
dropTrigger :: MonadDB m => Trigger -> m ()
dropTrigger trigger = do
  -- First, drop the trigger, as it is dependent on the function. See the comment in
  -- 'sqlDropTrigger'.
  -- TODO: Use 'withTransaction' here? That would mean adding MonadMask...
  runQuery_ $ sqlDropTrigger trigger
  runQuery_ . sqlDropTriggerFunction $ triggerFunction trigger

-- | Get all noninternal triggers from the database.
--
-- Run a query that returns all triggers associated with the given table and marked as
-- @tgisinternal = false@.
--
-- Note that, in the background, to get the trigger's @WHEN@ clause and the source code of
-- the attached function, the entire query that had created the trigger is received using
-- @pg_get_triggerdef(t.oid, true)::text@ and then parsed. The result of that call will be
-- decompiled and normalized, which means that it's likely not what the user had
-- originally typed.
--
-- @since 1.15.0
getDBTriggers :: forall m. MonadDB m => RawSQL () -> m [Trigger]
getDBTriggers tableName = do
  runQuery_ . sqlSelect "pg_trigger t" $ do
    sqlResult "t.tgname::text" -- name
    sqlResult "t.tgtype" -- smallint == int2 => (2 bytes)
    sqlResult "t.tgdeferrable" -- boolean
    sqlResult "t.tginitdeferred"-- boolean
    -- This gets the entire query that created this trigger. Note that it's decompiled and
    -- normalized, which means that it's likely not what the user actually typed. For
    -- example, if the original query had excessive whitespace in it, it won't be in this
    -- result.
    sqlResult "pg_get_triggerdef(t.oid, true)::text"
    sqlResult "p.proname::text" -- name
    sqlResult "p.prosrc" -- text
    sqlResult "c.relname::text"
    sqlJoinOn "pg_proc p" "t.tgfoid = p.oid"
    sqlJoinOn "pg_class c" "c.oid = t.tgrelid"
    sqlWhereEq "t.tgisinternal" False
    sqlWhereEq "c.relname" $ unRawSQL tableName
  fetchMany getTrigger
  where
    getTrigger :: (String, Int16, Bool, Bool, String, String, String, String) -> Trigger
    getTrigger (tgname, tgtype, tgdeferrable, tginitdeferrable, triggerdef, proname, prosrc, tblName) =
      Trigger { triggerTable = tableName'
              , triggerName = triggerBaseName (unsafeSQL tgname) tableName'
              , triggerEvents = trgEvents
              , triggerDeferrable = tgdeferrable
              , triggerInitiallyDeferred = tginitdeferrable
              , triggerWhen = tgrWhen
              , triggerFunction = TriggerFunction (unsafeSQL proname) (unsafeSQL prosrc)
              }
      where
        tableName' :: RawSQL ()
        tableName' = unsafeSQL tblName

        parseBetween :: Text -> Text -> Maybe (RawSQL ())
        parseBetween left right =
          let (prefix, match) = Text.breakOnEnd left $ Text.pack triggerdef
          in if Text.null prefix
             then Nothing
             else Just $ (rawSQL . fst $ Text.breakOn right match) ()

        -- Get the WHEN part of the query. Anything between WHEN and EXECUTE is what we
        -- want. The Postgres' grammar guarantees that WHEN and EXECUTE are always next to
        -- each other and in that order.
        tgrWhen :: Maybe (RawSQL ())
        tgrWhen = parseBetween "WHEN (" ") EXECUTE"

        -- Similarly, in case of UPDATE OF, the columns can be simply parsed from the
        -- original query. Note that UPDATE and UPDATE OF are mutually exclusive and have
        -- the same bit set in the underlying tgtype bit field.
        trgEvents :: Set TriggerEvent
        trgEvents =
          foldl' (\set (mask, event) ->
                    if testBit tgtype mask
                    then
                      Set.insert
                        (if event == TriggerUpdate
                         then maybe event trgUpdateOf $ parseBetween "UPDATE OF " " ON"
                         else event
                        )
                        set
                    else set
                 )
          Set.empty
          -- Taken from PostgreSQL sources: src/include/catalog/pg_trigger.h:
          [ (2, TriggerInsert) -- #define TRIGGER_TYPE_INSERT (1 << 2)
          , (3, TriggerDelete) -- #define TRIGGER_TYPE_DELETE (1 << 3)
          , (4, TriggerUpdate) -- #define TRIGGER_TYPE_UPDATE (1 << 4)
          ]

        trgUpdateOf :: RawSQL () -> TriggerEvent
        trgUpdateOf columnsSQL =
          let columns = map (unsafeSQL . Text.unpack) . Text.splitOn ", " $ unRawSQL columnsSQL
          in TriggerUpdateOf columns

