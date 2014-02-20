{-# LANGUAGE OverloadedStrings #-}

module Data.Etcd
    ( Client(..)
    , createClient

    , Node(..)

    , listKeys
    , getKey
    , putKey
    ) where


import           Data.Aeson hiding (Error)
import           Data.ByteString.Char8 (pack)

import           Control.Applicative
import           Control.Exception
import           Control.Monad

import           Network.HTTP.Conduit hiding (Response, path)


data Client = Client
    { leaderUrl :: String
    , machines :: [ String ]
    }



-- | The version prefix used in URLs. The current client supports v2.
versionPrefix :: String
versionPrefix = "v2"


buildUrl :: Client -> String -> String
buildUrl c p = leaderUrl c ++ "/" ++ versionPrefix ++ "/" ++ p


------------------------------------------------------------------------------
-- | Each response comes with an "action" field, which describes what kind of
-- action was performed.
data Action = GET | SET | DELETE | CREATE | EXPIRE | CAS | CAD
    deriving (Show, Eq, Ord)


instance FromJSON Action where
    parseJSON (String "get")              = return GET
    parseJSON (String "set")              = return SET
    parseJSON (String "delete")           = return DELETE
    parseJSON (String "create")           = return CREATE
    parseJSON (String "expire")           = return EXPIRE
    parseJSON (String "compareAndSwap")   = return CAS
    parseJSON (String "compareAndDelete") = return CAD
    parseJSON _                           = fail "Action"



------------------------------------------------------------------------------
-- | The server responds with this object to all successful requests.
data Response = Response
    { _resAction   :: Action
    , _resNode     :: Node
    , _resPrevNode :: Maybe Node
    } deriving (Show, Eq, Ord)


instance FromJSON Response where
    parseJSON (Object o) = Response
        <$> o .:  "action"
        <*> o .:  "node"
        <*> o .:? "prevNode"

    parseJSON _ = fail "Response"



------------------------------------------------------------------------------
-- | The server sometimes responds to errors with this error object.
data Error = Error
    deriving (Show, Eq, Ord)

instance FromJSON Error where
    parseJSON _ = return Error



data Node = Node
    { _nodeKey           :: String
    , _nodeValue         :: Maybe String
    , _nodeCreatedIndex  :: Int
    , _nodeModifiedIndex :: Int
    , _nodeNodes         :: Maybe [Node]
    } deriving (Show, Eq, Ord)


instance FromJSON Node where
    parseJSON (Object o) = Node
        <$> o .:  "key"
        <*> o .:? "value"
        <*> o .:  "createdIndex"
        <*> o .:  "modifiedIndex"
        <*> o .:? "nodes"

    parseJSON _ = fail "Response"



{-|---------------------------------------------------------------------------

Low-level HTTP interface

The functions here are used internally when sending requests to etcd. If the
server is running, the result is 'Either Error Response'. These functions may
throw an exception if the server is unreachable or not responding.

-}


-- A type synonym for a http response.
type HR = Either Error Response


httpGET :: String -> IO HR
httpGET url = do
    req  <- acceptJSON <$> parseUrl url
    body <- responseBody <$> (withManager $ httpLbs req)
    return $ maybe (Left Error) Right $ decode body

  where
    acceptHeader   = ("Accept","application/json")
    acceptJSON req = req { requestHeaders = acceptHeader : requestHeaders req }


httpPOST :: String -> [(String, String)] -> IO HR
httpPOST url params = do
    req' <- parseUrl url
    let req = urlEncodedBody (map (\(k,v) -> (pack k, pack v)) params) $ req'

    void $ withManager $ httpLbs req
    return $ Left Error


------------------------------------------------------------------------------
-- | Run a low-level HTTP request. Catch any exceptions and convert them into
-- a 'Left Error'.
runRequest :: IO HR -> IO HR
runRequest a = catch a (ignoreExceptionWith (return $ Left Error))

ignoreExceptionWith :: a -> SomeException -> a
ignoreExceptionWith a _ = a



{-|---------------------------------------------------------------------------

Public API

-}


-- | Create a new client and initialize it with a list of seed machines. The
-- list must be non-empty.
createClient :: [ String ] -> IO Client
createClient seed = return $ Client (head seed) seed


listKeys :: Client -> String -> IO [ Node ]
listKeys client path = do
    hr <- runRequest $ httpGET $ buildUrl client $ "keys/" ++ path
    case hr of
        Left _ -> return []
        Right res -> return $ [ _resNode res ]


getKey :: Client -> String -> IO (Maybe Node)
getKey client path = do
    hr <- runRequest $ httpGET $ buildUrl client $ "keys/" ++ path
    case hr of
        Left _ -> return Nothing
        Right res -> return $ Just $ _resNode res


putKey :: Client -> String -> String -> IO ()
putKey client path value = do
    void $ httpPOST (buildUrl client $ "keys/" ++ path) [("value", value)]
    return ()
