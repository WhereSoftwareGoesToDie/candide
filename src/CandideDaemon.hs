{-# LANGUAGE RecordWildCards #-}

module Main where

import           Control.Concurrent.Async
import qualified Data.ByteString.Char8     as S
import           Data.Monoid
import           Data.Word
import           Options.Applicative
import           System.Log.Logger

import           Candide.Server
import           Vaultaire.Program
import           Vaultaire.Types

data Options = Options
    { host           :: String
    , port           :: Word16
    , user           :: String
    , pass           :: String
    , debug          :: Bool
    , quiet          :: Bool
    , cacheFile      :: String
    , cacheFlushFreq :: Integer
    , origin         :: String
    , namespace      :: String
    }

helpfulParser :: ParserInfo Options
helpfulParser = info (helper <*> optionsParser) fullDesc

optionsParser :: Parser Options
optionsParser = Options <$> parseHost
                        <*> parsePort
                        <*> parseUser
                        <*> parsePass
                        <*> parseDebug
                        <*> parseQuiet
                        <*> parseCacheFile
                        <*> parseCacheFlushFreq
                        <*> parseOrigin
                        <*> parseNameSpace
  where
    parseHost = strOption $
           long "host"
        <> short 'H'
        <> metavar "HOSTNAME"
        <> value "localhost"
        <> showDefault
        <> help "PostgreSQL host name or IP address"

    parsePort = option auto $
           long "port"
        <> short 'p'
        <> metavar "PORT"
        <> value 5432
        <> showDefault
        <> help "PostgreSQL port"

    parseUser = strOption $
           long "host"
        <> short 'u'
        <> metavar "USER"
        <> help "PostgreSQL username"

    parsePass = strOption $
           long "pass"
        <> short 'P'
        <> metavar "PASSWORD"
        <> help "PostgreSQL password"

    parseDebug = switch $
           long "debug"
        <> short 'd'
        <> help "Output lots of debugging information"

    parseQuiet = switch $
           long "quiet"
        <> short 'q'
        <> help "Only emit warnings or fatal messages"

    parseCacheFile = strOption $
           long "cache-file"
        <> short 'c'
        <> value ""
        <> help "Location to read/write cached SourceDicts"

    -- This doesn't mean that marquised will rigorously flush the cache
    -- every `t` seconds. To clarify: marquised consideres flushing the
    -- cache after it finishes reading every spool file. If the cache
    -- hasn't been flushed in the last `t` seconds, it will be flushed
    -- before processing the next spool file.
    parseCacheFlushFreq = option auto $
           long "cache-flush-freq"
        <> short 't'
        <> help "Period of time to wait between cache writes, in seconds"
        <> value 42

    parseNameSpace = argument str (metavar "NAMESPACE")

    parseOrigin = argument str (metavar "ORIGIN")

defaultCacheLoc :: String -> String
defaultCacheLoc = (++) "/var/cache/marquise/"

main :: IO ()
main = do
    Options{..} <- execParser helpfulParser

    let level
          | debug     = Debug
          | quiet     = Quiet
          | otherwise = Normal

    quit <- initializeProgram "candided" level

    cacheFile' <- return $ case cacheFile of
        "" -> defaultCacheLoc origin
        x  -> x

    a <- runCandideDaemon host port user pass (Origin $ S.pack origin) namespace quit cacheFile' cacheFlushFreq

    -- wait forever
    wait a
    debugM "Main.main" "End"
