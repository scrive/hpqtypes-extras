module Database.PostgreSQL.PQTypes.Model.Function where

import Data.Monoid.Utils
import Database.PostgreSQL.PQTypes

data Function = Function
  { fnName :: RawSQL ()
  , fnBody :: RawSQL ()
  , fnReturns :: RawSQL ()
  , fnSecurity :: Security
  , fnSearchPath :: Maybe (RawSQL ())
  }
  deriving (Show)

data Security = Invoker | Definer
  deriving (Show)

-- | Turn the function into a SQL statement.
--
-- NB: If an existing function has the same name, it is replaced.
--
-- @since 1.xx.x.x
sqlCreateFunction :: Function -> RawSQL ()
sqlCreateFunction Function {..} =
  "CREATE OR REPLACE FUNCTION"
    <+> fnName
    <> "()"
    <+> returns
    <+> "AS $$"
    <+> fnBody
    <+> "$$"
    <+> "LANGUAGE PLPGSQL"
    <+> "VOLATILE"
    <+> security
    <+> "RETURNS NULL ON NULL INPUT"
    <+> searchPath
    <> ";"
  where
    returns = "RETURNS" <+> fnReturns
    security = case fnSecurity of
      Invoker -> "SECURITY INVOKER"
      Definer -> "SECURITY DEFINER"
    searchPath = case fnSearchPath of
      Nothing -> ""
      Just contents -> "SET search_path = " <> contents

-- | Build an SQL statement for dropping a function.
--
-- @since 1.xx.x.x
sqlDropFunction :: Function -> RawSQL ()
sqlDropFunction Function {..} =
  "DROP FUNCTION" <+> fnName <+> "RESTRICT"
