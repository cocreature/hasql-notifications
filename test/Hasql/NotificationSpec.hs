{-# LANGUAGE OverloadedStrings #-}
module Hasql.NotificationSpec
  ( spec
  ) where

import           Control.Applicative (pure)
import           Control.Concurrent
import           Control.Exception
import           Data.Either
import           Data.Maybe
import           Data.Text.Encoding (encodeUtf8)
import qualified Database.PostgreSQL.LibPQ as PQ
import           Hasql.Connection
import           Hasql.Notification
import           Hasql.Session
import           Test.Hspec



withDBConn :: (Connection -> IO a) -> IO a
withDBConn f =
  bracket (fmap (\(Right conn) -> conn) $
           acquire (settings "" 0 "postgres" "" "postgres"))
          release
          f


getNotNonBlock = getNotificationNonBlocking' withConn

withConn = withLibPQConnection
execute conn t = run (sql t) conn

spec :: Spec
spec =
  do around withDBConn $
       describe "getNotificationNonBlocking" $
       do it "should not return a notification" $
            \conn ->
              do _ <- execute conn "LISTEN channel"
                 notification <- getNotNonBlock conn
                 notification `shouldSatisfy` isNothing
     around withDBConn $
       describe "getNotification'" $
       do it "should return a notification" $
            \conn ->
              do
                 -- The lock ensures that getNotification can’t block the connection while it is waiting
                 lock <- newEmptyMVar
                 _ <- execute conn "LISTEN channel"
                 _ <-
                   forkIO $
                   do putStrLn "waiting for lock"
                      takeMVar lock
                      putStrLn "got lock"
                      threadDelay 2000000
                      res <- withConn conn (\c -> exec c "NOTIFY channel")
                      print res
                      putStrLn "notified"
                      pure ()
                 notification <-
                   getNotification' (\c f -> withConn c (test lock f))
                                    conn
                 notification `shouldSatisfy` isRight


test lock f c = do putStrLn "filling lock"
                   result <- tryPutMVar lock ()
                   putStrLn $  "put result: " ++ show result
                   x <- f c
                   putStrLn "performed op on libpq conn"
                   pure x

-- For some reason it seems to be important that the notify message is
-- send using this function rather than simply combining sendQuery w
-- ith getResult like hasql does. Otherwise random race conditions
-- occur in which threadWaitRead deadlocks.
exec h sql =
  do success <- PQ.sendQuery h sql
     if success
        then awaitResult h
        else error "PQsendQuery failed"
  where awaitResult h =
          do mfd <- PQ.socket h
             case mfd of
               Nothing -> error "Database.PostgreSQL.Simple.Internal.exec"
               Just fd ->
                 do -- Disabling threadWaitRead causes the bug
                    -- threadWaitRead fd
                    PQ.getResult h
