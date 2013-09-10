module Main where

import Data.Etcd


main = do
    print "Starting etcd client..."
    client <- createClient [ "http://127.0.0.1:4001" ]
    keys <- listKeys client "/_etcd/machines"
    print keys
    return ()
