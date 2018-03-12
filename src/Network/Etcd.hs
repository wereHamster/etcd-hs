{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

{-|

This module contains an implementation of the etcd client.

-}

module Network.Etcd
    ( Client
    , createClient

      -- * Types
    , Node(..)
    , Index
    , Key
    , Value
    , TTL

      -- * Low-level key operations
    , get
    , set
    , create
    , wait
    , waitIndex
    , waitRecursive
    , waitIndexRecursive

      -- * Directory operations
    , createDirectory
    , listDirectoryContents
    , listDirectoryContentsRecursive
    , removeDirectory
    , removeDirectoryRecursive
    ) where


import           Data.Aeson hiding (Value, Error)
import           Data.Time.Clock
import           Data.Time.LocalTime
import           Data.List
import           Data.Text (Text)
import qualified Data.Text as T
import           Data.Text.Encoding
import           Data.Monoid
import           Data.ByteString.Lazy (ByteString)

import           Control.Applicative
import           Control.Exception
import           Control.Monad

import           Network.HTTP.Conduit hiding (Response, path)

import           Prelude


-- | The 'Client' holds all data required to make requests to the etcd
-- cluster. You should use 'createClient' to initialize a new client.
data Client = Client
    { leaderUrl :: !Text
      -- ^ The URL to the leader. HTTP requests are sent to this server.
    }



-- | The version prefix used in URLs. The current client supports v2.
versionPrefix :: Text
versionPrefix = "v2"


-- | The URL to the given key.
keyUrl :: Client -> Key -> Text
keyUrl client key = leaderUrl client <> "/" <> versionPrefix <> "/keys/" <> key


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
data Error = Error !Text
    deriving (Show, Eq, Ord)


-- | The etcd index is a unique, monotonically-incrementing integer created for
-- each change to etcd. See etcd documentation for more details.
type Index = Int


-- | Keys are strings, formatted like filesystem paths (ie. slash-delimited
-- list of path components).
type Key = Text


-- | Values attached to leaf nodes are strings. If you want to store
-- structured data in the values, you'll need to encode it into a string.
type Value = Text


-- | TTL is specified in seconds. The server accepts negative values, but they
-- don't make much sense.
type TTL = Int


-- | The 'Node' corresponds to the node object as returned by the etcd API.
--
-- There are two types of nodes in etcd. One is a leaf node which holds
-- a value, the other is a directory node which holds zero or more child nodes.
-- A directory node can not hold a value, the two types are exclusive.
--
-- On the wire, the two are not really distinguished, except that the JSON
-- objects have different fields.
--
-- A node may be set to expire after a number of seconds. This is indicated by
-- the two fields 'ttl' and 'expiration'.

data Node = Node
    { _nodeKey           :: !Key
      -- ^ The key of the node. It always starts with a slash character (0x47).

    , _nodeCreatedIndex  :: !Index
      -- ^ A unique index, reflects the point in the etcd state machine at
      -- which the given key was created.

    , _nodeModifiedIndex :: !Index
      -- ^ Like '_nodeCreatedIndex', but reflects when the node was last
      -- changed.

    , _nodeDir           :: !Bool
      -- ^ 'True' if this node is a directory.

    , _nodeValue         :: !(Maybe Value)
      -- ^ The value is only present on leaf nodes. If the node is
      -- a directory, then this field is 'Nothing'.

    , _nodeNodes         :: !(Maybe [Node])
      -- ^ If this node is a directory, then these are its children. The list
      -- may be empty.

    , _nodeTTL           :: !(Maybe TTL)
      -- ^ If the node has TTL set, this is the number of seconds how long the
      -- node will exist.

    , _nodeExpiration    :: !(Maybe UTCTime)
      -- ^ If TTL is set, then this is the time when it expires.

    } deriving (Show, Eq, Ord)


instance FromJSON Node where
    parseJSON (Object o) = Node
        <$> o .:? "key" .!= "/"
        <*> o .:? "createdIndex" .!= 0
        <*> o .:? "modifiedIndex" .!= 0
        <*> o .:? "dir" .!= False
        <*> o .:? "value"
        <*> o .:? "nodes"
        <*> o .:? "ttl"
        <*> (fmap zonedTimeToUTC <$> (o .:? "expiration"))

    parseJSON _ = fail "Response"



{-|---------------------------------------------------------------------------

Low-level HTTP interface

The functions here are used internally when sending requests to etcd. If the
server is running, the result is 'Either Error Response'. These functions may
throw an exception if the server is unreachable or not responding.

-}


-- A type synonym for a http response.
type HR = Either Error Response

decodeResponseBody :: ByteString -> IO HR
decodeResponseBody body = do
    return $ case eitherDecode body of
        Left e  -> Left $ Error (T.pack e)
        Right n -> Right n


httpGET :: Text -> [(Text, Text)] -> IO HR
httpGET url params = do
    req'  <- acceptJSON <$> parseUrl (T.unpack url)
    let req = setQueryString (map (\(k,v) -> (encodeUtf8 k, Just $ encodeUtf8 v)) params) $ req'
    body <- responseBody <$> (withHttpManager $ httpLbs req)
    decodeResponseBody body

  where
    acceptHeader   = ("Accept","application/json")
    acceptJSON req = req { requestHeaders = acceptHeader : requestHeaders req }


httpPUT :: Text -> [(Text, Text)] -> IO HR
httpPUT url params = do
    req' <- parseUrl (T.unpack url)
    let req = urlEncodedBody (map (\(k,v) -> (encodeUtf8 k, encodeUtf8 v)) params) $ req'

    body <- responseBody <$> (withHttpManager $ httpLbs $ req { method = "PUT" })
    decodeResponseBody body


httpPOST :: Text -> [(Text, Text)] -> IO HR
httpPOST url params = do
    req' <- parseUrl (T.unpack url)
    let req = urlEncodedBody (map (\(k,v) -> (encodeUtf8 k, encodeUtf8 v)) params) $ req'

    body <- responseBody <$> (withHttpManager $ httpLbs $ req { method = "POST" })
    decodeResponseBody body


-- | Issue a DELETE request to the given url. Since DELETE requests don't have
-- a body, the params are appended to the URL as a query string.
httpDELETE :: Text -> [(Text, Text)] -> IO HR
httpDELETE url params = do
    req  <- parseUrl $ T.unpack $ url <> (asQueryParams params)
    body <- responseBody <$> (withHttpManager $ httpLbs $ req { method = "DELETE" })
    decodeResponseBody body

  where
    asQueryParams [] = ""
    asQueryParams xs = "?" <> mconcat (intersperse "&" (map (\(k,v) -> k <> "=" <> v) xs))


------------------------------------------------------------------------------
-- | Run a low-level HTTP request. Catch any exceptions and convert them into
-- a 'Left Error'.
runRequest :: IO HR -> IO HR
runRequest a = catch a (\(e :: SomeException) -> return $ Left $ Error $ T.pack $ show e)

runRequest' :: IO HR -> IO (Maybe Node)
runRequest' m = either (const Nothing) (Just . _resNode) <$> runRequest m


-- | Encode an optional TTL into a param pair.
ttlParam :: Maybe TTL -> [(Text, Text)]
ttlParam Nothing    = []
ttlParam (Just ttl) = [("ttl", T.pack $ show ttl)]



{-----------------------------------------------------------------------------

Public API

-}


-- | Create a new client and initialize it with a list of seed machines. The
-- list must be non-empty.
createClient :: [ Text ] -> IO Client
createClient seed = return $ Client (head seed)



{-----------------------------------------------------------------------------

Low-level key operations

-}

waitParam :: (Text, Text)
waitParam = ("wait","true")

waitRecursiveParam :: (Text, Text)
waitRecursiveParam = ("recursive","true")

waitIndexParam :: Index -> (Text, Text)
waitIndexParam i = ("waitIndex", (T.pack $ show i))


-- | Get the node at the given key.
get :: Client -> Key -> IO (Maybe Node)
get client key =
    runRequest' $ httpGET (keyUrl client key) []


-- | Set the value at the given key.
set :: Client -> Key -> Value -> Maybe TTL -> IO (Maybe Node)
set client key value mbTTL =
    runRequest' $ httpPUT (keyUrl client key) $
        [("value",value)] ++ ttlParam mbTTL


-- | Create a value in the given key. The key must be a directory.
create :: Client -> Key -> Value -> Maybe TTL -> IO Node
create client key value mbTTL = do
    hr <- runRequest $ httpPOST (keyUrl client key) $
        [("value",value)] ++ ttlParam mbTTL

    case hr of
        Left _ -> error "Unexpected error"
        Right res -> return $ _resNode res


-- | Wait for changes on the node at the given key.
wait :: Client -> Key -> IO (Maybe Node)
wait client key =
    runRequest' $ httpGET (keyUrl client key) [waitParam]


-- | Same as 'wait' but at a given index.
waitIndex :: Client -> Key -> Index -> IO (Maybe Node)
waitIndex client key index =
    runRequest' $ httpGET (keyUrl client key) $
        [waitParam, waitIndexParam index]


-- | Same as 'wait' but includes changes on children.
waitRecursive :: Client -> Key -> IO (Maybe Node)
waitRecursive client key =
    runRequest' $ httpGET (keyUrl client key) $
        [waitParam, waitRecursiveParam]


-- | Same as 'waitIndex' but includes changes on children.
waitIndexRecursive :: Client -> Key -> Index -> IO (Maybe Node)
waitIndexRecursive client key index =
    runRequest' $ httpGET (keyUrl client key) $
        [waitParam, waitIndexParam index, waitRecursiveParam]



{-----------------------------------------------------------------------------

Directories are non-leaf nodes which contain zero or more child nodes. When
manipulating directories one must include dir=true in the request params.

-}

dirParam :: [(Text, Text)]
dirParam = [("dir","true")]

recursiveParam :: [(Text, Text)]
recursiveParam = [("recursive","true")]


-- | Create a directory at the given key.
createDirectory :: Client -> Key -> Maybe TTL -> IO ()
createDirectory client key mbTTL =
    void $ runRequest $ httpPUT (keyUrl client key) $ dirParam ++ ttlParam mbTTL


-- | List all nodes within the given directory.
listDirectoryContents :: Client -> Key -> IO [Node]
listDirectoryContents client key = do
    hr <- runRequest $ httpGET (keyUrl client key) []
    case hr of
        Left _ -> return []
        Right res -> do
            let node = _resNode res
            case _nodeNodes node of
                Nothing -> return []
                Just children -> return children


-- | Same as 'listDirectoryContents' but includes all descendant nodes. Note
-- that directory 'Node's will not contain their children.
listDirectoryContentsRecursive :: Client -> Key -> IO [Node]
listDirectoryContentsRecursive client key = do
    hr <- runRequest $ httpGET (keyUrl client key) recursiveParam
    case hr of
        Left _ -> return []
        Right res -> do
            let node = _resNode res
                flatten n = n { _nodeNodes = Nothing }
                          : maybe [] (concatMap flatten) (_nodeNodes n)
            case _nodeNodes node of
                Nothing -> return []
                Just children -> return $ concatMap flatten children


-- | Remove the directory at the given key. The directory MUST be empty,
-- otherwise the removal fails. If you don't care about the keys within, you
-- can use 'removeDirectoryRecursive'.
removeDirectory :: Client -> Key -> IO ()
removeDirectory client key =
    void $ runRequest $ httpDELETE (keyUrl client key) dirParam


-- | Remove the directory at the given key, including all its children.
removeDirectoryRecursive :: Client -> Key -> IO ()
removeDirectoryRecursive client key =
    void $ runRequest $ httpDELETE (keyUrl client key) $ dirParam ++ recursiveParam


-- | withManager has been removed form http-conduit-2.3
withHttpManager :: (Manager -> IO a) -> IO a
withHttpManager f = do
    manager <- newManager tlsManagerSettings
    f manager
