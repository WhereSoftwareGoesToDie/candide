{-# LANGUAGE RecordWildCards #-}

module Main where

import qualified Data.ByteString.Char8 as BSC
import           Data.Monoid
import           Data.Word
import           Options.Applicative

import           Candide.Core
import           Vaultaire.Types

data Options = Options
    { host   :: String
    , port   :: Word16
    , user   :: String
    , pass   :: String
    , origin :: String
    }

helpfulParser :: ParserInfo Options
helpfulParser = info (helper <*> optionsParser) fullDesc

optionsParser :: Parser Options
optionsParser = Options <$> parseHost
                        <*> parsePort
                        <*> parseUser
                        <*> parsePass
                        <*> parseOrigin
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

    parseOrigin = argument str (metavar "ORIGIN")

main :: IO ()
main = do
    Options{..} <- execParser helpfulParser
    let origin' = Origin $ BSC.pack origin
    setupOrigin host port user pass origin'
    putStrLn "Ding!"
    putStrLn $ "Created origin " ++ show origin'
