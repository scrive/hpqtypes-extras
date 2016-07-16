module DB.Model.Migration (
    Migration(..)
  )  where

import Data.Int

import DB.Model.Table

-- | Migration object. Fields description:
-- * mgrTable is the table you're migrating
-- * mgrFrom is the version you're migrating from (you don't specify what
--   version you migrate TO, because version is always increased by 1, so
--   if mgrFrom is 2, that means that after that migration is run, table
--   version will equal 3
-- * mgrDo is actual body of a migration

data Migration m = Migration {
  mgrTable :: Table
, mgrFrom  :: Int32
, mgrDo    :: m ()
}
