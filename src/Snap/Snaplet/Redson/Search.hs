{-# LANGUAGE OverloadedStrings #-}

{-|

Ad-hoc Redis search backed by field indices.

-}

module Snap.Snaplet.Redson.Search
    ( PatternFunction
    , SearchTerm
    , prefixMatch
    , substringMatch
    , redisSearch
    )

where

import qualified Data.ByteString as B

import Database.Redis

import Snap.Snaplet.Redson.CRUD
import Snap.Snaplet.Redson.Metamodel
import Snap.Snaplet.Redson.Util


------------------------------------------------------------------------------
-- | A function which fill build Redis key pattern for certain way of
-- matching values of index field.
type PatternFunction = ModelName -> SearchTerm -> B.ByteString


-----------------------------------------------------------------------------
-- | Describe that field should somehow match the provided value.
type SearchTerm = (FieldName, FieldValue)


------------------------------------------------------------------------------
-- | Match prefixes.
prefixMatch :: PatternFunction
prefixMatch model (field, value) = B.append (modelIndex model field value) "*"


------------------------------------------------------------------------------
-- | Match substrings.
substringMatch :: PatternFunction
substringMatch model (field, value) =
    B.concat [model, ":", field, ":*", value, "*"]


------------------------------------------------------------------------------
-- | Redis action which returns list of instance id's matching given
-- search terms.
redisSearch :: ModelName
            -> Model
            -- ^ Model instances of which are being searched
            -> [SearchTerm]
            -- ^ List of requested field values
            -> PatternFunction
            -- ^ How to build pattern for matching keys
            -> Redis [[InstanceId]]
redisSearch mname model searchTerms patFunction =
    let
        -- Get list of ids which match single search term
        getTermIds pattern = do
          Right sets <- keys pattern
          case sets of
            [] -> return []
            _ -> do
              -- Hedis hangs when doing `suinion []`
              --
              -- TODO Maybe use sunionstore and perform further
              -- operations on Redis as well.
              Right ids <- sunion sets
              return ids
     in
       -- Try to get search results for every index field
       mapM (\s -> getTermIds (patFunction mname s))
                (filter (\(k, v) -> elem k $ indices model) searchTerms)
