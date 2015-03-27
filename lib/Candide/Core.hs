{-# LANGUAGE OverloadedStrings #-}

module Candide.Core where

import           Control.Applicative
import           Control.Monad
import           Data.Bifunctor
import           Data.Either
import qualified Data.HashMap.Strict                  as H
import           Data.Maybe
import           Data.Monoid
import           Data.Scientific
import           Data.Text                            (Text)
import           Data.Word
import           Database.PostgreSQL.Simple           as PG
import           Database.PostgreSQL.Simple.FromField
import           Database.PostgreSQL.Simple.FromRow
import           Database.PostgreSQL.Simple.HStore
import           Database.PostgreSQL.Simple.ToField
import           Pipes
import           Pipes.PostgreSQL.Simple              as PPG
import qualified Pipes.Prelude                        as P

import           Vaultaire.Types

instance FromRow SimplePoint where
    fromRow = do
        t <- field
        a <- field
        v <- field
        return $ SimplePoint (fromIntegral (a :: Integer))
                             (fromIntegral (t :: Integer))
                             (fromIntegral (v :: Integer))

instance ToField Origin where
    toField x = toField $ show x

instance FromField Address where
    fromField a b = do
        x <- fromField a b
        return $ Address $ round (x :: Scientific)

ctvContents :: [(Text, Text)] -> Either String SourceDict
ctvContents pairs = makeSourceDict $ H.fromList pairs

getContents :: PG.Connection -> Address -> IO (Maybe SourceDict)
getContents conn (Address addr) = do
    res <- concat <$> PG.query conn "SELECT sourcedict FROM metadata WHERE address = ?" (Only addr)
    case res of
        [] -> do
            putStrLn "LOL NO SD"
            return Nothing
        [sd'] -> case ctvContents (fromHStoreList sd') of
            Left err -> do
                putStrLn $ "LOL SHIT WENT DOWN GETTING DAT SD: " <> err
                return Nothing
            Right sd -> return $ Just sd
        x -> error "got more than one sourcedict from your address, should not get here"

candideConnection :: String -> Word16 -> String -> String -> Maybe Origin -> IO PG.Connection
candideConnection host port user pass origin = connect stuff
  where
    database = maybe "postgres" (const "foobar") origin
    stuff = ConnectInfo host port user pass database

setupOrigin :: String -> Word16 -> String -> String -> Origin -> IO ()
setupOrigin host port user pass origin = do
    conn <- candideConnection host port user pass Nothing
    void $ execute_ conn "CREATE DATABASE FOOBAR WITH ENCODING 'UTF8'"-- (Only origin)
    putStrLn "DID THE EXECUTE THING"
    conn' <- candideConnection host port user pass (Just origin)
    putStrLn "made the NEW CONNECTION"
    void $ execute_ conn' "CREATE EXTENSION IF NOT EXISTS hstore"
    begin conn'
    void $ execute_ conn' "CREATE TABLE data (timestamp numeric, address numeric, value numeric)"
    void $ execute_ conn' "CREATE TABLE metadata (address numeric, sourcedict hstore)"
    commit conn'

writeContents :: PG.Connection
              -> Address
              -> [(Text, Text)]
              -> IO ()
writeContents conn (Address addr) sd =
    void $ PG.execute conn "INSERT INTO metadata VALUES (?, ?)" (addr, HStoreList sd)

writeSimple :: PG.Connection
            -> SimplePoint
            -> IO ()
writeSimple conn (SimplePoint (Address addr) (TimeStamp ts) v) =
    void $ PG.execute conn "INSERT INTO data VALUES (?,?,?)" (ts, addr, v)

writeManySimple :: PG.Connection
                -> [SimplePoint]
                -> IO ()
writeManySimple conn ps' =
    let ps = map (\(SimplePoint (Address addr) (TimeStamp ts) v) -> (ts, addr, v)) ps'
    in void $ PG.executeMany conn "INSERT INTO data VALUES (?,?,?)" ps

enumerateOrigin :: PG.Connection
                -> Producer (Address, SourceDict) IO ()
enumerateOrigin conn =
    PPG.query_ conn "SELECT address,sourcedict FROM metadata" >-> P.map (second $ either error id . ctvContents . fromHStoreList)

readSimple :: PG.Connection
           -> Address
           -> TimeStamp
           -> TimeStamp
           -> Producer SimplePoint IO ()
readSimple conn (Address addr) (TimeStamp s) (TimeStamp e) = do
    x <- liftIO $ PG.formatQuery conn "SELECT * FROM data WHERE address = ? AND timestamp BETWEEN ? AND ?" (addr, s, e)
    liftIO $ print x
    liftIO $ putStrLn "all da shiz"
    PPG.query conn "SELECT * FROM data WHERE address = ? AND timestamp BETWEEN ? AND ?" (addr, s, e)

getEverything :: PG.Connection
              -> Producer (Scientific, Scientific, Scientific) IO ()
getEverything conn = PPG.query_ conn "SELECT * FROM data"
