{-# LANGUAGE AllowAmbiguousTypes, TypeApplications #-}
module Database.PostgreSQL.PQTypes.DerivingVia (
  -- | Helpers, to be used with `deriving via`.
    SQLEnum(..)
  , SQLEnumEncoding(..)
  , SQLEnumAsText(..)
  , SQLEnumAsTextEncoding(..)
  , isInjective  -- ^ for doctests
  ) where

import Control.Exception (SomeException(..), throwIO)
import Data.List.Extra (enumerate, nubSort)
import Data.Map.Strict (Map)
import Data.Text (Text)
import Data.Typeable
import Database.PostgreSQL.PQTypes
import qualified Data.Map.Strict as Map

-- | To be used in doctests to prove injectivity of encoding functions.
--
-- >>> isInjective (id :: Bool -> Bool)
-- True
--
-- >>> isInjective (\(_ :: Bool) -> False)
-- False
isInjective :: (Enum a, Bounded a, Eq a, Eq b) => (a -> b) -> Bool
isInjective f = null [ (a, b) | a <- enumerate, b <- enumerate, a /= b, f a == f b ]

-- | Internal helper; given a list of values, decompose it into a list of
-- intervals.
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

-- | Helper newtype to be used with `deriving via` to derive
-- `(PQFormat, ToSQL, FromSQL)` instances for enums.
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
  encodeEnum :: a -> SQLEnumType a
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
    case Map.lookup b decodeEnumMap of
      Nothing -> throwIO $ SomeException RangeError
        { reRange = intervals $ Map.keys (decodeEnumMap @a)
        , reValue = b
        }
      Just a -> return $ SQLEnum a


-- | A special case of `SQLEnum`, where the enum is to be encoded as text (and
-- SQLEnum can't be used because of the `Enum (SQLEnumType)` constraint).
newtype SQLEnumAsText a = SQLEnumAsText a

class (Enum a , Bounded a) => SQLEnumAsTextEncoding a where
  encodeEnumAsText :: a -> Text
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
    case Map.lookup text decodeEnumAsTextMap of
      Nothing -> throwIO $ SomeException InvalidValue
        { ivValue       = text
        , ivValidValues = Just $ Map.keys (decodeEnumAsTextMap @a)
        }
      Just a -> return $ SQLEnumAsText a
