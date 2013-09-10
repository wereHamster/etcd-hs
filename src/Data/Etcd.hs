{-# LANGUAGE OverloadedStrings #-}

module Data.Etcd where


import           Data.Aeson

import qualified Data.Map as M

import           Data.ByteString (ByteString)
import           Data.ByteString.Char8 (pack)

import           Data.Conduit
import           Data.Conduit.Binary (sinkFile)

import           Control.Applicative
import           Control.Exception
import           Control.Monad

import           Network.HTTP.Conduit
import           Network.HTTP.Types.Method


data Client = Client
    { leaderUrl :: String
    , machines :: [ String ]
    }


data Tree
    = Node
        { key   :: String
        , index :: Int
        }

    | Leaf
        { key   :: String
        , index :: Int

        , value :: String
        }

    deriving (Show, Eq, Ord)


parseNode o = Node
    <$> o .: "key"
    <*> o .: "index"

parseLeaf o = Leaf
    <$> o .: "key"
    <*> o .: "index"
    <*> o .: "value"


instance FromJSON Tree where
    parseJSON (Object o) = parseLeaf o <|> parseNode o
    parseJSON _          = fail "Data.Etcd.Tree"


getJSON :: (FromJSON a) => String -> IO (Maybe a)
getJSON url = do
    req  <- acceptJSON <$> parseUrl url
    body <- responseBody <$> (withManager $ httpLbs req)
    return $ decode body

  where

    acceptJSON req = req { requestHeaders = acceptHeader : requestHeaders req }
    acceptHeader = ("Accept","application/json")


postUrlEncoded :: String -> [(String, String)] -> IO ()
postUrlEncoded url params = do
    req' <- parseUrl url
    let req = urlEncodedBody (map (\(k,v) -> (pack k, pack v)) params) $ req'

    void $ withManager $ httpLbs req


ignoreExceptionWith :: a -> SomeException -> a
ignoreExceptionWith a _ = a


listKeys :: Client -> String -> IO [ Tree ]
listKeys client path = (flip catch) (ignoreExceptionWith (return [])) $ do
    response <- getJSON $ leaderUrl client ++ "/v1/keys" ++ path
    return $ maybe [] id response


getKey :: Client -> String -> IO (Maybe Tree)
getKey client path = (flip catch) (ignoreExceptionWith (return Nothing)) $ do
    getJSON $ leaderUrl client ++ "/v1/keys" ++ path


putKey :: Client -> String -> String -> IO ()
putKey client path value = do
    postUrlEncoded (leaderUrl client ++ "/v1/keys" ++ path) [("value", value)]


-- | Create a new client and initialize it with a list of seed machines. The
-- list must be non-empty.
createClient :: [ String ] -> IO Client
createClient machines = return $ Client (head machines) machines
