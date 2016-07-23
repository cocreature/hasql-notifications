{-# LANGUAGE CPP #-}
{-# LANGUAGE LambdaCase #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  Hasql.Notification
-- Copyright   :  (c) 2016 Moritz Kiefer
--                (c) 2011-2015 Leon P Smith
--                (c) 2012 Joey Adams
-- License     :  MIT
--
-- Maintainer  :  Nikita Volkov <nikita.y.volkov@mail.ru>
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
     , getNotificationNonBlocking
     , getBackendPID
     ) where

import           Control.Exception (throwIO, try)
import           Control.Monad (join,void)
import qualified Data.ByteString as B
import qualified Database.PostgreSQL.LibPQ as PQ
import           GHC.IO.Exception (IOException(..),IOErrorType(ResourceVanished))
import           Hasql.Connection
import           System.Posix.Types (CPid)
#if defined(mingw32_HOST_OS)
import           Control.Concurrent (threadDelay)
#else
import           GHC.Conc (atomically)
import           Control.Concurrent (threadWaitReadSTM)
#endif

data Notification = Notification
   { notificationPid     :: !CPid
   , notificationChannel :: !B.ByteString
   , notificationData    :: !B.ByteString
   }

convertNotice :: PQ.Notify -> Notification
convertNotice not
    = Notification { notificationPid     = PQ.notifyBePid not
                   , notificationChannel = PQ.notifyRelname not
                   , notificationData    = PQ.notifyExtra not }

-- | Returns a single notification.  If no notifications are
-- available, 'getNotification' blocks until one arrives.
--
-- This uses 'withLibPQConnection' under the hood and thereby also
-- needs exclusive access to the connection while it is waiting. All
-- other access to the connection will block until the call
-- returns. Note that PostgreSQL does not deliver notifications while
-- a connection is inside a transaction.
getNotification :: Connection -> IO (Either IOError Notification)
getNotification conn = join $ withLibPQConnection conn fetch
  where
    funcName = "Hasql.Notification.getNotification"

    fetch c = do
        PQ.notifies c >>= \case
          Just msg -> return (return $! (Right $! convertNotice msg))
          Nothing -> do
              PQ.socket c >>= \case
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
                -- This case fixes the race condition above.   By registering
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
                              Left  err -> return (Left (setLoc err))
                              Right _   -> loop

#endif

    loop = join $ withLibPQConnection conn $ \c -> do
             void $ PQ.consumeInput c
             fetch c

    setLoc :: IOError -> IOError
    setLoc err = err { ioe_location = funcName }

    fdError :: IOError
    fdError = IOError {
                     ioe_handle      = Nothing,
                     ioe_type        = ResourceVanished,
                     ioe_location    = funcName,
                     ioe_description = "failed to fetch file descriptor (did the connection time out?)",
                     ioe_errno       = Nothing,
                     ioe_filename    = Nothing
                   }

-- | Non-blocking variant of 'getNotification'.   Returns a single notification,
-- if available.   If no notifications are available,  returns 'Nothing'.
getNotificationNonBlocking :: Connection -> IO (Maybe Notification)
getNotificationNonBlocking conn =
    withLibPQConnection conn $ \c -> do
        PQ.notifies c >>= \case
          Just msg -> return $! Just $! convertNotice msg
          Nothing -> do
              void $ PQ.consumeInput c
              PQ.notifies c >>= \case
                Just msg -> return $! Just $! convertNotice msg
                Nothing  -> return Nothing

-- | Returns the process 'CPid' of the backend server process
-- handling this connection.
--
-- The backend PID is useful for debugging purposes and for comparison
-- to NOTIFY messages (which include the PID of the notifying backend
-- process). Note that the PID belongs to a process executing on the
-- database server host, not the local host!
getBackendPID :: Connection -> IO CPid
getBackendPID conn = withLibPQConnection conn PQ.backendPID
