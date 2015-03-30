{-# LANGUAGE OverloadedStrings #-}

module Candide.Core where

import Blaze.ByteString.Builder.ByteString
import           Control.Applicative
import           Control.Monad
import           Data.Bifunctor
import           Data.Bits
import           Data.Char
import           Data.Either
import qualified Data.HashMap.Strict                  as H
import           Data.Int
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
import           Database.PostgreSQL.Simple.ToRow
import           Pipes
import           Pipes.PostgreSQL.Simple              as PPG
import qualified Pipes.Prelude                        as P

import           Vaultaire.Types

instance ToField Origin where
    toField (Origin x) = Plain (fromByteString $ "o_" <> x)

instance ToField Address where
    toField (Address x) = toField $ x `shift` (-1)

instance ToField TimeStamp where
    toField (TimeStamp x) = toField x

instance ToRow SimplePoint where
    toRow (SimplePoint a t v') = let v = fromIntegral v' :: Int64
                                 in  [ toField a, toField t, toField v]

instance FromField Address where
    fromField a b = Address <$> fromIntegral <$> (fromField a b :: Conversion Int64)

instance FromField SourceDict where
    fromField a b = either error id <$> ctvContents <$> fromHStoreList <$> fromField a b

instance FromRow SimplePoint where
    fromRow = do
        a <- field
        t <- field
        v <- field
        return $ SimplePoint (fromIntegral ((a `shift` 1) :: Int64))
                             (fromIntegral (t :: Integer))
                             (fromIntegral (v :: Integer))

extendifyAddress :: Address -> Bool -> Address
extendifyAddress x           False = x
extendifyAddress (Address x) True  = Address (x `setBit` 0)

ctvContents :: [(Text, Text)] -> Either String SourceDict
ctvContents pairs = makeSourceDict $ H.fromList pairs

getContents :: PG.Connection -> Address -> IO (Maybe SourceDict)
getContents conn (Address addr) = do
    res <- concat <$> PG.query conn "SELECT sourcedict FROM metadata WHERE address = ?" (Only addr)
    case res of
        [] -> return Nothing
        [sd'] -> case ctvContents (fromHStoreList sd') of
            Left err -> error $ "Error decoding sourcedict: " <> show sd' <> " error: " <> err
            Right sd -> return $ Just sd
        x -> error $ "Got more than one sourcedict from address, should not happen. Got: " <> show x

candideConnection :: String -> Word16 -> String -> String -> Maybe Origin -> IO PG.Connection
candideConnection host port user pass origin = putStrLn database >> connect stuff
  where
    database  = maybe "postgres" prepare origin
    stuff     = ConnectInfo host port user pass database
    prepare x = "o_" <> map toLower (show x)

setupOrigin :: String -> Word16 -> String -> String -> Origin -> IO ()
setupOrigin host port user pass origin = do
    conn <- candideConnection host port user pass Nothing
    void $ execute conn "CREATE DATABASE ? WITH ENCODING 'UTF8'" (Only origin)
    conn' <- candideConnection host port user pass (Just origin)
    void $ execute_ conn' "CREATE EXTENSION IF NOT EXISTS hstore"
    begin conn'
    void $ execute_ conn' "CREATE TABLE simple   (address bigint, timestamp bigint, value bigint,   CONSTRAINT simple_addr_ts   PRIMARY KEY(address, timestamp))"
    void $ execute_ conn' "CREATE TABLE extended (address bigint, timestamp bigint, value bytea,    CONSTRAINT extended_addr_ts PRIMARY KEY(address, timestamp))"
    void $ execute_ conn' "CREATE TABLE metadata (address bigint, sourcedict hstore, extended bool, CONSTRAINT metadata_addr    PRIMARY KEY(address))"
    void $ execute_ conn' $ "CREATE OR REPLACE RULE simple_ignore_duplicate_inserts AS ON INSERT TO simple " <>
                            "WHERE (EXISTS (SELECT 1 FROM simple WHERE address = NEW.address AND timestamp = NEW.timestamp)) " <>
                            "DO INSTEAD NOTHING"
    commit conn'

writeContents :: PG.Connection
              -> Address
              -> [(Text, Text)]
              -> IO ()
writeContents conn addr sd =
    void $ PG.execute conn "INSERT INTO metadata VALUES (?, ?)" (addr, HStoreList sd)

writeSimple :: PG.Connection
            -> SimplePoint
            -> IO ()
writeSimple conn p =
    void $ PG.execute conn "INSERT INTO simple VALUES (?,?,?)" p

writeManySimple :: PG.Connection
                -> [SimplePoint]
                -> IO ()
writeManySimple conn ps =
    void $ PG.executeMany conn "INSERT INTO simple VALUES (?,?,?)" ps

enumerateOrigin :: PG.Connection
                -> Producer (Address, SourceDict) IO ()
enumerateOrigin conn =
    PPG.query_ conn "SELECT * FROM metadata" >-> P.map convert
  where
    convert (addr, sd, extended) = (extendifyAddress addr extended, sd)

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
