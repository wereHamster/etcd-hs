module Main where

import           Test.Hspec

import           Data.Etcd
import           Data.Maybe

import           Control.Monad.Trans



main :: IO ()
main = hspec spec


spec :: Spec
spec = do

    describe "getKey" $ do
        it "should return the key node if present" $ do
            client <- createClient [ "http://127.0.0.1:4001" ]
            node <- getKey client "/_etcd/machines"
            node `shouldSatisfy` isJust

    describe "putKey" $ do
        it "should set the key" $ do
            client <- createClient [ "http://127.0.0.1:4001" ]
            putKey client "/test/key" "value"
            node <- getKey client "/test/key"
            node `shouldSatisfy` isJust
            (fmap _nodeValue node) `shouldBe` (Just $ Just "value")
