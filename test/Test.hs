{-# LANGUAGE OverloadedStrings #-}

module Main where

import           Test.Hspec

import           Data.Char
import           Data.Maybe
import           Data.Text (Text)
import qualified Data.Text as T
import           Data.Monoid

import           Control.Applicative
import           Control.Concurrent
import qualified Control.Concurrent.Async as Async
import           Control.Monad
import           Control.Monad.Random (getRandomR, evalRandIO)
import           Control.Monad.Trans

import           Network.Etcd


main :: IO ()
main = hspec spec


randomKey :: IO Text
randomKey = do
    rnd <- evalRandIO (sequence $ repeat $ getRandomR (0, 61))
    return $ T.pack $ take 13 $ map alnum rnd
  where
    alnum :: Int -> Char
    alnum x
        | x < 26    = chr ((x     ) + 65)
        | x < 52    = chr ((x - 26) + 97)
        | x < 62    = chr ((x - 52) + 48)
        | otherwise = error $ "Out of range: " ++ show x


setup :: IO (Client, Text)
setup = do
    client <- createClient [ "http://127.0.0.1:4001" ]
    key    <- randomKey

    return (client, key)


expectNode :: Maybe Node -> IO Node
expectNode = maybe (error "Expected Node") return

shouldBeLeaf :: Node -> Value -> Expectation
shouldBeLeaf node value = _nodeValue node `shouldBe` (Just value)

shouldBeDirectory :: Node -> Expectation
shouldBeDirectory node = _nodeDir node `shouldBe` True


spec :: Spec
spec = parallel $ do

    describe "Key Space Operations" $ do
        it "Get the root directory node" $ do
            (client, key) <- setup
            node <- expectNode =<< get client "/"
            shouldBeDirectory node

        it "Setting the value of a key" $ do
            (client, key) <- setup
            node <- expectNode =<< set client key "value" Nothing
            node `shouldBeLeaf` "value"

        it "Using key TTL" $ do
            (client, key) <- setup
            set client key "value" (Just 5)
            node <- expectNode =<< get client key
            node `shouldBeLeaf` "value"

            threadDelay $ 6 * 1000 * 1000
            node <- get client key
            node `shouldSatisfy` isNothing

        it "Changing the value of a key" $ do
            (client, key) <- setup
            set client key "value" Nothing
            node <- expectNode =<< set client key "value2" Nothing
            node `shouldBeLeaf` "value2"

        it "Get the value of a key" $ do
            (client, _) <- setup
            void $ fromJust <$> get client "/_etcd/machines"

        it "Wait for any change on a key" $ do
            (client, key) <- setup
            a <- Async.async $ wait client key
            threadDelay $ 2 * 1000 * 1000
            Async.async $ set client key "value" Nothing
            node <- expectNode =<< Async.wait a
            node `shouldBeLeaf` "value"

        it "Wait for a change on a key at a given index" $ do
            (client, key) <- setup
            Just n <- set client key "value" Nothing
            let i = _nodeModifiedIndex n
            node <- expectNode =<< waitIndex client key i
            node `shouldBeLeaf` "value"

        it "Wait for any change on a key recursively" $ do
            (client, key) <- setup
            a <- Async.async $ waitRecursive client key
            threadDelay $ 2 * 1000 * 1000
            Async.async $ set client (key <> "/child") "value" Nothing
            node <- expectNode =<< Async.wait a
            node `shouldBeLeaf` "value"

        it "Wait for a change on a key at a given index recursively" $ do
            (client, key) <- setup
            Just n <- set client (key <> "/child") "value" Nothing
            let i = _nodeModifiedIndex n
            node <- expectNode =<< waitIndexRecursive client key i
            node `shouldBeLeaf` "value"

        it "Creating directories" $ do
            (client, key) <- setup
            createDirectory client key Nothing
            node <- expectNode =<< get client key
            shouldBeDirectory node

        it "Deleting a directory" $ do
            (client, key) <- setup
            createDirectory client key Nothing
            removeDirectory client key

        it "Listing a directory" $ do
            (client, key) <- setup
            createDirectory client key Nothing
            set client (key <> "/one") "value" Nothing
            nodes <- listDirectoryContents client key
            length nodes `shouldBe` 1
            _nodeValue (head nodes) `shouldBe` (Just "value")

        it "Listing a directory recursively" $ do
            (client, key) <- setup
            createDirectory client key Nothing
            createDirectory client (key <> "/one") Nothing
            set client (key <> "/one/two") "value" Nothing
            set client (key <> "/one/three") "value" Nothing
            nodes <- listDirectoryContentsRecursive client key
            length nodes `shouldBe` 3
            _nodeValue (head nodes) `shouldBe` Nothing
            _nodeValue (last nodes) `shouldBe` (Just "value")

        it "Deleting a directory recursively" $ do
            (client, key) <- setup
            createDirectory client key Nothing
            set client (key <> "/one") "value" Nothing
            removeDirectoryRecursive client key

        it "Atomically Creating In-Order Keys" $ do
            (client, key) <- setup
            createDirectory client key Nothing
            node <- create client key "value" Nothing
            node `shouldBeLeaf` "value"

            nodes <- listDirectoryContents client key
            length nodes `shouldBe` 1
            _nodeValue (head nodes) `shouldBe` (Just "value")
