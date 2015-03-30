module Main where

import           Control.Applicative
import           Control.Monad
import           Control.Monad.Reader
import qualified Data.ByteString                 as BS
import           Data.List.Split
import           Data.Monoid
import qualified Data.Vector.Storable               as V
import           Data.Vector.Storable.ByteString
import           Data.Word
import           Foreign.Ptr
import           Foreign.Storable
import           Options.Applicative
import           System.Directory

import           Candide.Core
import           Vaultaire.Types

data ReplayOpts = ReplayOpts
    { pgsqlHost    :: String
    , pgsqlPort    :: Word16
    , pgsqlUser    :: String
    , pgsqlPass    :: String
    , oldNamespace :: String
    , replayOrigin :: Origin
    }

parseReplayOpts :: Parser ReplayOpts
parseReplayOpts = ReplayOpts
                <$> strOption
                    (long "pg-host"
                     <> value "127.0.0.1"
                     <> metavar "PGSQL_HOST")
                <*> option auto
                    (long "pg-port"
                     <> value 5432
                     <> metavar "PGSQL_PORT")
                <*> strOption
                    (long "pg-user"
                     <> metavar "USERNAME")
                <*> strOption
                    (long "pg-pass"
                     <> metavar "PASSWORD")
                <*> strOption
                    (short 'm'
                     <> metavar "OLD_NAMESPACE")
                <*> option auto
                    (short 'o'
                     <> metavar "ORIGIN")


data Point = Point { address :: !Word64
                   , time    :: !Word64
                   , payload :: !Word64
                   } deriving (Show, Eq)

instance Storable Point where
    sizeOf _    = 24
    alignment _ = 8
    peek ptr =
        Point <$> peek (castPtr ptr)
              <*> peek (ptr `plusPtr` 8)
              <*> peek (ptr `plusPtr` 16)
    poke ptr (Point a t p) =  do
        poke (castPtr ptr) a
        poke (ptr `plusPtr` 8 ) t
        poke (ptr `plusPtr` 16 ) p

instance Ord Point where
    -- Compare time first, then address. This way we can de-deplicate by
    -- comparing adjacent values.
    compare a b =
        case compare (time a) (time b) of
            EQ -> compare (address a) (address b)
            c  -> c

doAllTheThings :: ReplayOpts -> IO ()
doAllTheThings (ReplayOpts host port user pass namespace origin) = do
    c <- candideConnection host port user pass $ Just origin
    let basePath     = "/var/spool/marquise/" <> namespace
    let pointsPath   = basePath <> "/points/new/"
    fileChunks <- chunksOf 1 <$> (getDirectoryContents pointsPath >>= filterM doesFileExist . fmap (pointsPath <>))
    let coerce (Point addr ts v) = SimplePoint (Address addr) (TimeStamp ts) v
    forM_ fileChunks $ \f -> do
        points <- byteStringToVector <$> BS.concat <$> mapM BS.readFile f
        liftIO $ putStrLn $ "Size: " <> show (V.length points)
        writeManySimple c $ map coerce $ V.toList points

main :: IO ()
main = do
    opts <- execParser $ info parseReplayOpts desc
    doAllTheThings opts
  where
    desc = fullDesc <>
           progDesc "Copies data from spool files into candide"
