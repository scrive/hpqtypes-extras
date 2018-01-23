module Database.PostgreSQL.PQTypes.Checks.CheckOptions (
  CheckOptions(..),
  ) where

data CheckOptions = EnforceMandatoryPrimaryKeys
  deriving Eq
