module Candide.Server (
    runCandideDaemon
) where

import           Control.Applicative
import           Control.Concurrent              (threadDelay)
import           Control.Concurrent.Async
import qualified Control.Concurrent.Async.Lifted as AL
import           Control.Concurrent.MVar
import           Control.Exception               (throw)
import           Control.Monad
import           Control.Monad.Error
import           Control.Monad.State.Lazy
import           Data.Attoparsec.ByteString.Lazy (Parser)
import           Data.Attoparsec.Combinator      (eitherP)
import qualified Data.Attoparsec.Lazy            as Parser
import           Data.ByteString                 (ByteString)
import qualified Data.ByteString                 as BS
import           Data.ByteString.Builder         (Builder, byteString,
                                                  toLazyByteString)
import qualified Data.ByteString.Char8           as BSC
import qualified Data.ByteString.Lazy            as L
import qualified Data.HashMap.Strict             as H
import           Data.List
import           Data.Maybe
import           Data.Monoid
import           Data.Packer
import qualified Data.Set                        as S
import           Data.Time.Clock
import           Data.Word
import           Database.PostgreSQL.Simple      as PG
import           Pipes
import           Pipes.Attoparsec                (parsed)
import qualified Pipes.ByteString                as PB
import           Pipes.Group                     (FreeF (..), FreeT (..))
import qualified Pipes.Group                     as PG
import qualified Pipes.Lift                      as P
import qualified Pipes.Prelude                   as P
import           System.IO
import           System.Log.Logger

import           Candide.Core
import           Marquise.Classes
import           Marquise.Client
import           Marquise.Types
import           Vaultaire.Types

data ContentsRequest = ContentsRequest Address SourceDict
    deriving Show

runCandideDaemon :: String -> Word16 -> String -> String -> Origin -> String -> MVar () -> String -> Integer -> IO (Async ())
runCandideDaemon host port user pass origin namespace shutdown cache_file cache_flush_period = async $ do
    infoM "Server.runCandideDaemon" $ "Reading SourceDict cache from " ++ cache_file
    init_cache <- withFile cache_file ReadWriteMode $ \h -> do
        result <- fromWire <$> BSC.hGetContents h
        case result of
            Left e -> do
                warningM "Server.runCandideDaemon" $
                    concat ["Error decoding hash file: "
                           , show e
                           , " Continuing with empty initial cache"
                           ]
                return emptySourceCache
            Right cache -> do
                debugM "Server.runCandideDaemon" $
                    concat ["Read "
                           , show (sizeOfSourceCache cache)
                           , " hashes from source dict cache."
                           ]
                return cache
    infoM "Server.runCandideDaemon" "Connecting to Candide"
    conn <- candideConnection host port user pass (Just origin)
    infoM "Server.runCandideDaemon" "Candide daemon started"

    (points_loop, final_cache) <- do
        sn <- makeSpoolName namespace
        debugM "Server.runCandideDaemon" "Creating spool directories"
        createDirectories sn
        debugM "Server.runCandideDaemon" "Starting point transmitting thread"
        points_loop <- AL.async (sendPoints conn sn shutdown)
        currTime <- do
            link points_loop
            debugM "Server.runCandideDaemon" "Starting contents transmitting thread"
            getCurrentTime
        final_cache <- sendContents conn sn init_cache cache_file cache_flush_period currTime shutdown
        return (points_loop, final_cache)

   -- debugM "Server.runCandideDaemon" "Send loop shut down gracefully, writing out cache"
  --  S.writeFile cache_file $ toWire final_cache

    debugM "Server.runCandideDaemon" "Waiting for points loop thread"
    AL.wait points_loop

sendPoints :: PG.Connection -> SpoolName -> MVar () -> IO ()
sendPoints conn sn shutdown = do
    nexts <- nextPoints sn
    case nexts of
        Just (bytes, seal) -> do
            debugM "Server.sendPoints" "Got points, starting transmission pipe"
            runEffect $ for (breakInToChunks bytes) sendChunk
            debugM "Server.sendPoints" "Transmission complete, cleaning up"
            seal
        Nothing -> threadDelay idleTime

    done <- isJust <$> tryReadMVar shutdown
    unless done (sendPoints conn sn shutdown)
  where
    sendChunk chunk = liftIO $ do
        let size = show . BSC.length $ chunk
        debugM "Server.sendPoints" $ "Sending chunk of " ++ size ++ " bytes."
        let points = P.toList $ yield (SimpleBurst chunk) >-> decodeSimple
        writeManySimple conn points

sendContents :: PG.Connection
             -> SpoolName
             -> SourceDictCache
             -> String
             -> Integer
             -> UTCTime
             -> MVar ()
             -> IO SourceDictCache
sendContents conn sn initial cache_file cache_flush_period flush_time shutdown = do
        nexts <- nextContents sn
        (final, newFlushTime) <- case nexts of
            Just (bytes, seal) ->  do
                debugM "Server.sendContents" $
                    concat
                        [ "Got contents, starting transmission pipe with "
                        , show $ sizeOfSourceCache initial
                        , " cached sources."
                        ]
                reqs <- parseContentsRequests bytes
                notSeen <- filterSeen reqs initial
                newHashes <- S.unions <$> forM notSeen (return . S.singleton . hashRequest)
                let final' = S.foldl (flip insertSourceCache) initial newHashes
                sendSourceDictUpdate conn notSeen

                newFlushTime' <- do
                  debugM "Server.sendContents" "Contents transmission complete, cleaning up."
                  debugM "Server.sendContents" $
                      concat
                          [ "Saw "
                          , show $ sizeOfSourceCache final' - sizeOfSourceCache initial
                          , " new sources."
                          ]
                  seal
                  currTime <- getCurrentTime
                  if currTime > flush_time
                      then do
                          debugM "Server.setContents" "Performing periodic cache writeout."
                          BSC.writeFile cache_file $ toWire final'
                          return $ addUTCTime (fromInteger cache_flush_period) currTime
                      else do
                          debugM "Server.sendContents" $ concat ["Next cache flush at ", show flush_time, "."]
                          return flush_time
                return (final', newFlushTime')

            Nothing -> do
                threadDelay idleTime
                return (initial, flush_time)

        done <- isJust <$> tryReadMVar shutdown

        if done
        then return final
        else sendContents conn sn final cache_file cache_flush_period newFlushTime shutdown

  where
    hashRequest (ContentsRequest _ sd) = hashSource sd
    seen cache req = memberSourceCache (hashRequest req) cache
    filterSeen reqs cache = do
        let (prevSeen, notSeen) = partition (seen cache) reqs
        forM_ prevSeen $ \(ContentsRequest addr _) ->
            liftIO $ debugM "Server.filterSeen" $ "Seen source dict with address " ++ show addr ++ " before, ignoring."
        return notSeen
    sendSourceDictUpdate conn reqs = do
        forM_ reqs $ \(ContentsRequest addr _) ->
            liftIO (debugM "Server.sendContents" $ "Sending contents update for " ++ show addr)
        writeManyContents conn $ map reqToTuple reqs
    reqToTuple (ContentsRequest addr (SourceDict sd)) = (addr, H.toList sd)

parseContentsRequests :: Monad m => L.ByteString -> m [ContentsRequest]
parseContentsRequests bs = P.toListM $
    parsed parseContentsRequest (PB.fromLazy bs)
    >>= either (throw . fst) return

parseContentsRequest :: Parser ContentsRequest
parseContentsRequest = do
    addr <- fromWire <$> Parser.take 8
    len <- runUnpacking getWord64LE <$> Parser.take 8
    source_dict <- fromWire <$> Parser.take (fromIntegral len)
    case ContentsRequest <$> addr <*> source_dict of
        Left e -> fail (show e)
        Right request -> return request

idleTime :: Int
idleTime = 1000000 -- 1 second

breakInToChunks :: Monad m => L.ByteString -> Producer BSC.ByteString m ()
breakInToChunks bs =
    chunkBuilder (parsed parsePoint (PB.fromLazy bs))
    >>= either (throw . fst) return

-- Take a producer of (Int, Builder), where Int is the number of bytes in the
-- builder and produce chunks of n bytes.
--
-- This could be done with explicit recursion and next, but, then we would not
-- get to apply a fold over a FreeT stack of producers. This is almost
-- generalizable, at a stretch.
chunkBuilder :: Monad m => Producer (Int, Builder) m r -> Producer BSC.ByteString m r
chunkBuilder = PG.folds (<>) mempty (L.toStrict . toLazyByteString)
             -- Fold over each producer of counted Builders, turning it into
             -- a contigous strict ByteString ready for transmission.
             . builderChunks idealBurstSize
             -- Split the builder producer into FreeT
  where
    builderChunks :: Monad m
                  => Int
                  -- ^ The size to split a stream of builders at
                  -> Producer (Int, Builder) m r
                  -- ^ The input producer
                  -> FreeT (Producer Builder m) m r
                  -- ^ The FreeT delimited chunks of that producer, split into
                  --   the desired chunk length
    builderChunks max_size p = FreeT $ do
        -- Try to grab the next value from the Producer
        x <- next p
        return $ case x of
            Left r -> Pure r
            Right (a, p') -> Free $ do
                -- Pass the re-joined Producer to go, which will yield values
                -- from it until the desired chunk size is reached.
                p'' <- go max_size (yield a >> p')
                -- The desired chunk size has been reached, loop and try again
                -- with the rest of the stream (possibly empty)
                return (builderChunks max_size p'')

    -- We take a Producer and pass along its values until we've passed along
    -- enough bytes (at least the initial bytes_left).
    --
    -- When done, returns the remainder of the unconsumed Producer
    go :: Monad m
       => Int
       -> Producer (Int, Builder) m r
       -> Producer Builder m (Producer (Int, Builder) m r)
    go bytes_left p =
        if bytes_left < 0
            then return p
            else do
                x <- lift (next p)
                case x of
                    Left r ->
                        return . return $ r
                    Right ((size, builder), p') -> do
                        yield builder
                        go (bytes_left - size) p'

-- Parse a single point, returning the size of the point and the bytes as a
-- builder.
parsePoint :: Parser (Int, Builder)
parsePoint = do
    packet <- Parser.take 24

    case extendedSize packet of
        Just len -> do
            -- We must ensure that we get this many bytes now, or attoparsec
            -- will just backtrack on us. We do this with a dummy parser inside
            -- an eitherP
            --
            -- This is only to get good error messages.
            extended <- eitherP (Parser.take len) (return ())
            case extended of
                Left bytes ->
                    let b = byteString packet <> byteString bytes
                    in return (24 + len, b)
                Right () ->
                    fail "not enough bytes in alleged extended burst"
        Nothing ->
            return (24, byteString packet)

-- Return the size of the extended segment, if the point is an extended one.
extendedSize :: BSC.ByteString -> Maybe Int
extendedSize packet = flip runUnpacking packet $ do
    addr <- Address <$> getWord64LE
    if isAddressExtended addr
        then do
            unpackSkip 8
            Just . fromIntegral <$> getWord64LE -- length
        else
            return Nothing

-- A burst should be, at maximum, very close to this size, unless the user
-- decides to send a very long extended point.
idealBurstSize :: Int
idealBurstSize = 1048576
