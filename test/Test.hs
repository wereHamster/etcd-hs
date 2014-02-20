module Main where

import           Test.Hspec

import           Data.Char
import           Data.Etcd
import           Data.Maybe

import           Control.Applicative
import           Control.Concurrent
import           Control.Monad.Random (getRandomR, evalRandIO)
import           Control.Monad.Trans



main :: IO ()
main = hspec spec


randomKey :: IO String
randomKey = do
    rnd <- evalRandIO (sequence $ repeat $ getRandomR (0, 61))
    return $ take 13 $ map alnum rnd
  where
    alnum :: Int -> Char
    alnum x
        | x < 26    = chr ((x     ) + 65)
        | x < 52    = chr ((x - 26) + 97)
        | x < 62    = chr ((x - 52) + 48)
        | otherwise = error $ "Out of range: " ++ show x


setup :: IO (Client, String)
setup = do
    client <- createClient [ "http://127.0.0.1:4001" ]
    key    <- randomKey

    return (client, key)


spec :: Spec
spec = parallel $ do

    describe "Key Space Operations" $ do
        describe "Setting the value of a key" $ do
            it "should return the new node" $ do
                (client, key) <- setup
                node <- putKey client key "value" Nothing
                node `shouldSatisfy` isJust

        describe "Using key TTL" $ do
            it "should remove the key after it has expired" $ do
                (client, key) <- setup
                putKey client key "value" (Just 1)
                node <- getKey client key
                node `shouldSatisfy` isJust
                (fmap _nodeValue node) `shouldBe` (Just $ Just "value")

                threadDelay $ 2 * 1000 * 1000
                node <- getKey client key
                node `shouldSatisfy` isNothing

        describe "Changing the value of a key" $ do
            it "should return the new node" $ do
                (client, key) <- setup
                putKey client key "value" Nothing
                node <- putKey client key "value2" Nothing
                node `shouldSatisfy` isJust
                (fmap _nodeValue node) `shouldBe` (Just $ Just "value2")

        describe "Get the value of a key" $ do
            it "should return the node if present" $ do
                (client, _) <- setup
                node <- getKey client "/_etcd/machines"
                node `shouldSatisfy` isJust
