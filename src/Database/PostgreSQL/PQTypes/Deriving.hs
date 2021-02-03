{-# LANGUAGE AllowAmbiguousTypes #-}
module Database.PostgreSQL.PQTypes.Deriving (
  -- * Helpers, to be used with @deriving via@ (@-XDerivingVia@).
    SQLEnum(..)
  , SQLEnumEncoding(..)
  , SQLEnumAsText(..)
  , SQLEnumAsTextEncoding(..)
    -- * For use in doctests.
  , isInjective
  ) where

import Control.Exception (SomeException(..), throwIO)
import Data.List.Extra (enumerate, nubSort)
import Data.Map.Strict (Map)
import Data.Text (Text)
import Data.Typeable
import Database.PostgreSQL.PQTypes
import qualified Data.Map.Strict as Map

-- | Helper newtype to be used with @deriving via@ to derive @(PQFormat, ToSQL,
-- FromSQL)@ instances for enums, given an instance of 'SQLEnumEncoding'.
--
-- /Hint:/ non-trivial 'Enum' instances can be derived using the 'generic-data'
-- package!
--
-- >>> :{
-- data Colours = Blue | Black | Red | Mauve
--   deriving (Eq, Show, Enum, Bounded)
-- instance SQLEnumEncoding Colours where
--   type SQLEnumType Colours = Int16
--   encodeEnum = \case
--     Blue  -> 1
--     Black -> 7
--     Red   -> 2
--     Mauve -> 6
-- :}
--
-- /Note:/ To get SQL-specific instances use @DerivingVia@:
--
-- @
-- data Colours = ...
--   ...
--   deriving (PQFormat, ToSQL, FromSQL) via SQLEnum Colours
-- @
--
-- >>> isInjective (encodeEnum @Colours)
-- True
--
-- >>> decodeEnum @Colours 7
-- Right Black
--
-- >>> decodeEnum @Colours 42
-- Left [(1,2),(6,7)]
newtype SQLEnum a = SQLEnum a

class
  ( -- The semantic type needs to be finitely enumerable,
    Enum a
  , Bounded a
  -- and the target type has to be an enum with an SQL representation.
  , FromSQL (SQLEnumType a)
  , ToSQL (SQLEnumType a)
  , PQFormat (SQLEnumType a)
  -- Miscellaneous constraints:
  , Enum (SQLEnumType a)
  , Ord (SQLEnumType a)
  , Show (SQLEnumType a)
  , Typeable (SQLEnumType a)
  ) => SQLEnumEncoding a where
  type SQLEnumType a
  -- | Encode @a@ as an SQL compatible type.
  encodeEnum :: a -> SQLEnumType a

  -- | Decode SQL compatible type to an @a@. If the conversion fails, a list of
  -- valid ranges is returned instead.
  --
  -- /Note:/ The default implementation looks up the value in 'decodeEnumMap'
  -- and can be overwritten for performance if necessary.
  decodeEnum :: SQLEnumType a -> Either [(SQLEnumType a, SQLEnumType a)] a
  decodeEnum b = maybe (Left . intervals $ Map.keys (decodeEnumMap @a)) Right
               $ Map.lookup b (decodeEnumMap @a)

  -- | We include the definition of the inverse map as part of the
  -- 'SQLEnumEncoding' instance to ensure it is only computed once.
  decodeEnumMap :: Map (SQLEnumType a) a
  decodeEnumMap = Map.fromList [ (encodeEnum a, a) | a <- enumerate ]

instance SQLEnumEncoding a => PQFormat (SQLEnum a) where
  pqFormat = pqFormat @(SQLEnumType a)

instance SQLEnumEncoding a => ToSQL (SQLEnum a) where
  type PQDest (SQLEnum a) = PQDest (SQLEnumType a)
  toSQL (SQLEnum a) = toSQL $ encodeEnum a

instance SQLEnumEncoding a => FromSQL (SQLEnum a) where
  type PQBase (SQLEnum a) = PQBase (SQLEnumType a)
  fromSQL base = do
    b <- fromSQL base
    case decodeEnum b of
      Left validRange -> throwIO $ SomeException RangeError
        { reRange = validRange
        , reValue = b
        }
      Right a -> return $ SQLEnum a

-- | A special case of 'SQLEnum', where the enum is to be encoded as text
-- ('SQLEnum' can't be used because of the 'Enum' constraint on the domain of
-- 'encodeEnum').
--
-- >>> :{
-- data Person = Alfred | Bertrand | Charles
--   deriving (Eq, Show, Enum, Bounded)
-- instance SQLEnumAsTextEncoding Person where
--   encodeEnumAsText = \case
--     Alfred -> "alfred"
--     Bertrand -> "bertrand"
--     Charles -> "charles"
-- :}
--
-- /Note:/ To get SQL-specific instances use @DerivingVia@:
--
-- @
-- data Person = ...
--   ...
--   deriving (PQFormat, ToSQL, FromSQL) via SQLEnumAsText Person
-- @
--
-- >>> isInjective (encodeEnumAsText @Person)
-- True
--
-- >>> decodeEnumAsText @Person "bertrand"
-- Right Bertrand
--
-- >>> decodeEnumAsText @Person "batman"
-- Left ["alfred","bertrand","charles"]
newtype SQLEnumAsText a = SQLEnumAsText a

class (Enum a , Bounded a) => SQLEnumAsTextEncoding a where
  -- | Encode @a@ as 'Text'.
  encodeEnumAsText :: a -> Text

  -- | Decode 'Text' to an @a@. If the conversion fails, a list of valid values
  -- is returned instead.
  --
  -- /Note:/ The default implementation looks up the value in
  -- 'decodeEnumAsTextMap' and can be overwritten for performance if necessary.
  decodeEnumAsText :: Text -> Either [Text] a
  decodeEnumAsText text = maybe (Left $ Map.keys (decodeEnumAsTextMap @a)) Right
                        $ Map.lookup text (decodeEnumAsTextMap @a)

  -- | We include the inverse map as part of the 'SQLEnumTextEncoding' instance
  -- to ensure it is only computed once.
  decodeEnumAsTextMap :: Map Text a
  decodeEnumAsTextMap = Map.fromList [ (encodeEnumAsText a, a) | a <- enumerate ]

instance SQLEnumAsTextEncoding a => PQFormat (SQLEnumAsText a) where
  pqFormat = pqFormat @Text

instance SQLEnumAsTextEncoding a => ToSQL (SQLEnumAsText a) where
  type PQDest (SQLEnumAsText a) = PQDest Text
  toSQL (SQLEnumAsText a) = toSQL $ encodeEnumAsText a

instance SQLEnumAsTextEncoding a => FromSQL (SQLEnumAsText a) where
  type PQBase (SQLEnumAsText a) = PQBase Text
  fromSQL base = do
    text <- fromSQL base
    case decodeEnumAsText text of
      Left validValues -> throwIO $ SomeException InvalidValue
        { ivValue       = text
        , ivValidValues = Just validValues
        }
      Right a -> return $ SQLEnumAsText a

-- | To be used in doctests to prove injectivity of encoding functions.
--
-- >>> isInjective (id :: Bool -> Bool)
-- True
--
-- >>> isInjective (\(_ :: Bool) -> False)
-- False
isInjective :: (Enum a, Bounded a, Eq a, Eq b) => (a -> b) -> Bool
isInjective f = null [ (a, b) | a <- enumerate, b <- enumerate, a /= b, f a == f b ]

-- | Internal helper: given a list of values, decompose it into a list of
-- intervals.
--
-- >>> intervals [42,2,1,0,3,88,-1,43,42]
-- [(-1,3),(42,43),(88,88)]
--
-- prop> nubSort xs == concatMap (\(l,r) -> [l .. r]) (intervals xs)
intervals :: forall  a . (Enum a, Ord a) => [a] -> [(a, a)]
intervals as = case nubSort as of
  [] -> []
  (first : ascendingRest) -> accumIntervals (first, first) ascendingRest
  where
    accumIntervals :: (a, a) -> [a] -> [(a, a)]
    accumIntervals (lower, upper) [] = [(lower, upper)]
    accumIntervals (lower, upper) (first' : ascendingRest') = if succ upper == first'
      then accumIntervals (lower, first') ascendingRest'
      else (lower, upper) : accumIntervals (first', first') ascendingRest'

-- $setup
-- >>> import Data.Int
