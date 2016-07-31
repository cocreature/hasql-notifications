{-# LANGUAGE CPP #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE RankNTypes #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  Hasql.Notification
-- Copyright   :  (c) 2016 Moritz Kiefer
--                (c) 2011-2015 Leon P Smith
--                (c) 2012 Joey Adams
-- License     :  BSD3
--
-- Maintainer  :  Moritz Kiefer <moritz.kiefer@purelyfunctional.org>
--
-- Support for receiving asynchronous notifications via PostgreSQL's
-- Listen/Notify mechanism.  See
-- <http://www.postgresql.org/docs/9.4/static/sql-notify.html> for more
-- information.
--
-- Note that on Windows,  @getNotification@ currently uses a polling loop
-- of 1 second to check for more notifications,  due to some inadequacies
-- in GHC's IO implementation and interface on that platform.   See GHC
-- issue #7353 for more information.  While this workaround is less than
-- ideal,  notifications are still better than polling the database directly.
-- Notifications do not create any extra work for the backend,  and are
-- likely cheaper on the client side as well.
--
-- <http://hackage.haskell.org/trac/ghc/ticket/7353>
--
-----------------------------------------------------------------------------

module Hasql.Notification
     ( Notification(..)
     , getNotification
     , getNotification'
     , getNotificationNonBlocking
     , getNotificationNonBlocking'
     , getBackendPID
     ) where

import           Control.Applicative (pure)
import           Control.Exception (try)
import           Control.Monad (void)
import           Control.Monad.Except
import qualified Data.ByteString as B
import qualified Database.PostgreSQL.LibPQ as PQ
import           GHC.IO.Exception (IOException(..),IOErrorType(ResourceVanished))
import           Hasql.Connection
import           System.Posix.Types (CPid,Fd)
#if defined(mingw32_HOST_OS)
import           Control.Concurrent (threadDelay)
#else
import           GHC.Conc (atomically)
import           Control.Concurrent (threadWaitReadSTM,threadDelay,forkIO)
#endif

-- | A single notification returned by PostgreSQL
--
-- This is kept separate from 'PQ.Notify' to keep control over the
-- API.
data Notification =
  Notification {notificationPid :: !CPid -- ^ PID of notifying server process
               ,notificationChannel :: !B.ByteString -- ^ notification channel name
               ,notificationData :: !B.ByteString -- ^ notification payload string
               }
  deriving (Show,Eq,Ord)

convertNotice :: PQ.Notify -> Notification
convertNotice notification =
  Notification {notificationPid = PQ.notifyBePid notification
               ,notificationChannel = PQ.notifyRelname notification
               ,notificationData = PQ.notifyExtra notification}

-- | Returns a single notification. If no notifications are available,
-- 'getNotification' blocks until one arrives.
--
-- The connection is not blocked while waiting for notifications. Note
-- that PostgreSQL does not deliver notifications while a connection
-- is inside a transaction.
getNotification :: Connection -> IO (Either IOError Notification)
getNotification = getNotification' withLibPQConnection

-- | Returns a single notification.  If no notifications are
-- available, 'getNotification' blocks until one arrives.
--
-- The function is used to gain access to the raw @libpq@
-- 'PQ.Connection'. It is only used for nonblocking reads so the
-- connection should not be blocked. Note that PostgreSQL does not
-- deliver notifications while a connection is inside a transaction.
getNotification' :: (forall a. c -> (PQ.Connection -> IO a) -> IO a) -> c -> IO (Either IOError Notification)
getNotification' withConnection conn = join $ withConnection conn fetch
  where
    funcName = "Hasql.Notification.getNotification"
    fetch :: PQ.Connection -> IO (IO (Either IOError Notification))
    fetch c = do
        mmsg <- PQ.notifies c
        case mmsg of
          Just msg -> return (return $! Right $! convertNotice msg)
          Nothing -> do
              mfd <- PQ.socket c
              case mfd of
                Nothing  -> return (return (Left fdError))
#if defined(mingw32_HOST_OS)
                -- threadWaitRead doesn't work for sockets on Windows, so just
                -- poll for input every second (PQconsumeInput is non-blocking).
                --
                -- We could call select(), but FFI calls can't be interrupted
                -- with async exceptions, whereas threadDelay can.
                Just _fd -> do
                  return (threadDelay 1000000 >> loop)
#else
                -- Technically there's a race condition that is usually benign.
                -- If the connection is closed or reset after we drop the
                -- lock,  and then the fd index is reallocated to a new
                -- descriptor before we call threadWaitRead,  then
                -- we could end up waiting on the wrong descriptor.
                --
                -- Now, if the descriptor becomes readable promptly,  then
                -- it's no big deal as we'll wake up and notice the change
                -- on the next iteration of the loop.   But if are very
                -- unlucky,  then we could end up waiting a long time.
                --
                -- By registering
                -- our interest in the descriptor before we drop the lock,
                -- there is no opportunity for the descriptor index to be
                -- reallocated on us.
                --
                -- (That is, assuming there isn't concurrently executing
                -- code that manipulates the descriptor without holding
                -- the lock... but such a major bug is likely to exhibit
                -- itself in an at least somewhat more dramatic fashion.)
                Just fd  -> do
                  (waitRead, _) <- threadWaitReadSTM fd
                  return $ try (atomically waitRead) >>= \case
                    Left err -> return (Left (setLoc err))
                    Right _ -> loop
#endif
    loop =
      join $
      withConnection conn $
      \c ->
        do void $ PQ.consumeInput c
           fetch c

    setLoc :: IOError -> IOError
    setLoc err = err {ioe_location = funcName}

    fdError :: IOError
    fdError =
      IOError {ioe_handle = Nothing
              ,ioe_type = ResourceVanished
              ,ioe_location = funcName
              ,ioe_description =
                 "failed to fetch file descriptor (did the connection time out?)"
              ,ioe_errno = Nothing
              ,ioe_filename = Nothing}

-- | Non-blocking variant of 'getNotification'.
--
-- Returns a single notification, if available. If no notifications
-- are available, returns 'Nothing'.
getNotificationNonBlocking :: Connection -> IO (Maybe Notification)
getNotificationNonBlocking = getNotificationNonBlocking' withLibPQConnection

-- | Non-blocking variant of 'getNotification''.
--
-- Returns a single notification, if available. If no notifications
-- are available, returns 'Nothing'. The function is used to gain
-- access to the raw @libpq@ 'PQ.Connection'.
getNotificationNonBlocking' :: (forall a. c -> (PQ.Connection -> IO a) -> IO a) -> c -> IO (Maybe Notification)
getNotificationNonBlocking' withConnection conn =
  withConnection conn $
  \c ->
    do PQ.notifies c >>=
         \case
           Just msg -> return $! Just $! convertNotice msg
           Nothing ->
             do void $ PQ.consumeInput c
                PQ.notifies c >>=
                  \case
                    Just msg -> return $! Just $! convertNotice msg
                    Nothing -> return Nothing

-- | Returns the process 'CPid' of the backend server process
-- handling this connection.
--
-- The backend PID is useful for debugging purposes and for comparison
-- to NOTIFY messages (which include the PID of the notifying backend
-- process). Note that the PID belongs to a process executing on the
-- database server host, not the local host!
getBackendPID :: Connection -> IO CPid
getBackendPID conn = withLibPQConnection conn PQ.backendPID
