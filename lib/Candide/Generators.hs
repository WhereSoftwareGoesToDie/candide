module Candide.Generators where

import           Control.Applicative
import           Test.QuickCheck

import           Vaultaire.Types

instance Arbitrary SimplePoint where
    arbitrary = simpleStream

simpleStream :: Gen SimplePoint
simpleStream = SimplePoint <$> resize 10000 arbitrary
                           <*> resize 97    arbitrary
                           <*> resize 100   arbitrary
