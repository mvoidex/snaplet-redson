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
    , redisIndex
    , redisSearch'
    )

where

import Control.Applicative ((<$>))

import Control.Monad (void, when, forM)
import Control.Monad.IO.Class (liftIO)

import Control.Concurrent.MVar

import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as C8

import qualified Data.Text as T
import qualified Data.Text.Encoding as E

import Data.Maybe (catMaybes)

import Data.Monoid

import Database.Redis

import Snap.Snaplet.Redson.Snapless.CRUD
import Snap.Snaplet.Redson.Snapless.Metamodel

import qualified Snap.Snaplet.Redson.Search.NGram as NGram
import qualified Text.Search.NGram as NG

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
-- | Redis action which returns list of matching instance id's for
-- every search term.
redisSearch :: Model
            -- ^ Model instances of which are being searched
            -> [SearchTerm]
            -- ^ List of requested index field values
            -> PatternFunction
            -- ^ How to build pattern for matching keys
            -> Redis [[InstanceId]]
redisSearch model searchTerms patFunction =
    let
        mname = modelName model
        -- Get list of ids which match single search term
        getTermIds pattern = do
          Right sets <- keys pattern
          case sets of
            -- Do not attempt sunion with no arguments.
            [] -> return []
            _ -> do
              -- TODO Maybe use sunionstore and perform further
              -- operations on Redis as well.
              Right ids <- sunion sets
              return ids
     in
       -- Try to get search results for every index field
       mapM (getTermIds . (patFunction mname)) searchTerms

-- | Index fields
redisIndex
    :: MVar (NGram.Index InstanceId)
    -- ^ Index to fill
    -> MVar Bool
    -- ^ Is index created
    -> ModelName
    -- ^ Name of model
    -> [FieldName]
    -- ^ Fields to index
    -> Redis ()

redisIndex mVar mFull mName fNames = do
    let mName' = C8.unpack mName
    createdIx <- liftIO $ swapMVar mFull True
    when (not createdIx) $ do
        Right ([Just maxIdStr]) <- mget [C8.pack $ "global:" ++ mName' ++ ":id"]
        let maxId = read $ C8.unpack maxIdStr
        strIds <- (mconcat . concat) <$> (forM [1..maxId] $ \i -> do
            Right fVals <- hmget (C8.pack $ mName' ++ ":" ++ show i) fNames
            let
                decode' = T.unpack . E.decodeUtf8
                values = map decode' $ filter (not . B.null) $ catMaybes fVals
                i' = C8.pack $ show i
            return $ map (\v -> NG.value (NG.ngram 3) v i') values)
        NGram.initializeIndex mVar strIds

-- | Search for term, return list of matching instances
redisSearch'
    :: MVar (NGram.Index InstanceId)
    -> ModelName
    -> [FieldName]
    -> B.ByteString
    -> Redis [[InstanceId]]

redisSearch' mVar mName fNames term = do
    ix <- fmap NGram.searched $ liftIO $ readMVar mVar
    let
        term' = T.unpack $ E.decodeUtf8 term
        matches = map fst $ NG.search ix (NG.ngram 3) term'
        mName' = C8.unpack mName
        modelId i = C8.pack $ mName' ++ ":" ++ C8.unpack i
    preciseMatches <- catMaybes <$> (forM matches $ \m -> do
        Right matched <- hmget (modelId m) fNames
        let matchFields = map (T.unpack . E.decodeUtf8) (catMaybes matched)
        return $ if NG.matchPrecise matchFields term'
            then Just m
            else Nothing)
    return [preciseMatches]
