{-# LANGUAGE OverloadedStrings #-}

module Candide.Core where

import           Blaze.ByteString.Builder.ByteString
import           Control.Applicative
import           Control.Monad
import           Data.Bifunctor
import           Data.Bits
import           Data.Char
import           Data.Either
import qualified Data.HashMap.Strict                  as H
import           Data.Int
import           Data.List
import           Data.List.Extra
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
import           Database.PostgreSQL.Simple.Types
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
    toRow (SimplePoint a t v') = let v = prepareWord v'
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
        return $ SimplePoint (Address $ returnToWord a `shift` 1)
                             (TimeStamp $ returnToWord t)
                             (returnToWord v)

-- Postgres only has signed integers
-- We define some renamed fromIntegral calls to get around this

prepareWord :: Word64 -> Int64
prepareWord = fromIntegral

returnToWord :: Int64 -> Word64
returnToWord = fromIntegral

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
    void $ execute_ conn' "CREATE TABLE metadata (address bigint, sourcedict hstore,                CONSTRAINT metadata_addr    PRIMARY KEY(address))"
    void $ execute_ conn' "CREATE INDEX search_metadata ON metadata USING GIST (sourcedict)"
    void $ execute_ conn' $ "CREATE OR REPLACE RULE simple_ignore_duplicate_inserts AS ON INSERT TO simple " <>
                            "WHERE (EXISTS (SELECT 1 FROM simple WHERE address = NEW.address AND timestamp = NEW.timestamp)) " <>
                            "DO INSTEAD NOTHING"
    void $ execute_ conn' $ "CREATE OR REPLACE RULE metadata_last_write_wins AS ON INSERT TO metadata " <>
                            "WHERE (EXISTS (SELECT 1 FROM metadata WHERE address = NEW.address)) " <>
                            "DO INSTEAD UPDATE metadata SET sourcedict = NEW.sourcedict"
    commit conn'

writeContents :: PG.Connection
              -> Address
              -> [(Text, Text)]
              -> IO ()
writeContents conn addr sd =
    void $ PG.execute conn "INSERT INTO metadata VALUES (?, ?)" (addr, HStoreList sd)

writeManyContents :: PG.Connection
                  -> [(Address, [(Text, Text)])]
                  -> IO ()
writeManyContents conn pairs =
    void $ PG.executeMany conn "INSERT INTO metadata VALUES (?, ?)" $
        -- We reverse because nubOrd keeps the first occurence and we want the last
        map (second HStoreList) $ nubOrdOn fst $ reverse pairs


writeSimple :: PG.Connection
            -> SimplePoint
            -> IO ()
writeSimple conn p =
    void $ PG.execute conn "INSERT INTO simple VALUES (?,?,?)" p

writeManySimple :: PG.Connection
                -> [SimplePoint]
                -> IO ()
writeManySimple conn ps =
    void $ PG.executeMany conn "INSERT INTO simple VALUES (?,?,?)" $ nubOrdOn addrTs ps
  where
    addrTs (SimplePoint addr ts _) = (addr, ts)

enumerateOrigin :: PG.Connection
                -> Producer (Address, SourceDict) IO ()
enumerateOrigin conn =
    PPG.query_ conn "SELECT * FROM metadata"

readSimple :: PG.Connection
           -> Address
           -> TimeStamp
           -> TimeStamp
           -> Producer SimplePoint IO ()
readSimple conn (Address addr) (TimeStamp s) (TimeStamp e) =
    PPG.query conn "SELECT * FROM simple WHERE address = ? AND timestamp BETWEEN ? AND ?" (addr, s, e)

searchTags :: PG.Connection
           -> [(Text, Text)]
           -> Producer (Address, SourceDict) IO ()
searchTags conn tags = do
    let (keys, values) = bimap PGArray PGArray $ unzip tags
    q <- liftIO $ formatQuery conn "SELECT * FROM metadata WHERE sourcedict -> ? = ?" (keys, values)
    liftIO $ print q
    PPG.query conn "SELECT * FROM metadata WHERE sourcedict -> ? = ?" (keys, values)
