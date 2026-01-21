module Database.PostgreSQL.PQTypes.Model.Function where

import Data.Map.Strict qualified as M
import Data.Monoid.Utils
import Data.Text qualified as T
import Database.PostgreSQL.PQTypes

data Function = Function
  { fnName :: RawSQL ()
  , fnBody :: RawSQL ()
  , fnReturns :: RawSQL ()
  , fnSecurity :: Security
  , fnConfigurationParameters :: M.Map T.Text (RawSQL ())
  }
  deriving (Show)

instance Eq Function where
  f1 == f2 =
    fnName f1 == fnName f2
      && fnBody f1 == fnBody f2
      && fnReturns f1 == fnReturns f2
      && fnSecurity f1 == fnSecurity f2
      && fnConfigurationParameters f1 == fnConfigurationParameters f2

data Security = Invoker | Definer
  deriving (Show, Eq)

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
    <> fnBody
    <> "$$"
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
    searchPath =
      foldr
        (\(param, value) prev -> "SET" <+> unsafeSQL (T.unpack param) <+> "=" <+> value <+> prev)
        (unsafeSQL "")
        $ M.toList fnConfigurationParameters

-- | Build an SQL statement for dropping a function.
--
-- @since 1.xx.x.x
sqlDropFunction :: Function -> RawSQL ()
sqlDropFunction Function {..} =
  "DROP FUNCTION" <+> fnName <+> "RESTRICT"
