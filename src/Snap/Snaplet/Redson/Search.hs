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

import Data.Monoid (mconcat)

import Database.Redis

import Snap.Snaplet.Redson.Snapless.CRUD
import Snap.Snaplet.Redson.Snapless.Metamodel

import qualified Text.Search.NGram as NGram

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
    -> ModelName
    -- ^ Name of model
    -> [FieldName]
    -- ^ Fields to index
    -> Redis ()

redisIndex mVar mName fNames = do
    let mName' = C8.unpack mName
    emptyIx <- liftIO $ isEmptyMVar mVar
    when emptyIx $ do
        Right ([Just maxIdStr]) <- mget [C8.pack $ "global:" ++ mName' ++ ":id"]
        let maxId = read $ C8.unpack maxIdStr
        strIds <- (mconcat . concat) <$> (forM [1..maxId] $ \i -> do
            Right fVals <- hmget (C8.pack $ mName' ++ ":" ++ show i) fNames
            let
                decode' = T.unpack . E.decodeUtf8
                values = map decode' $ filter (not . B.null) $ catMaybes fVals
                i' = C8.pack $ show i
            return $ map (\v -> NGram.value (NGram.ngram 3) v i') values)
        void $ liftIO $ putMVar mVar $ strIds

-- | Search for term, return list of matching instances
redisSearch'
    :: MVar (NGram.Index InstanceId)
    -> ModelName
    -> B.ByteString
    -> Redis [[InstanceId]]

redisSearch' mVar mName term = do
    ix <- liftIO $ readMVar mVar
    let
        term' = T.unpack $ E.decodeUtf8 term
        matches = map fst $ NGram.search ix (NGram.ngram 3) term'
        mName' = C8.unpack mName
    liftIO $ putStrLn $ "Searching for: " ++ term'
    preciseMatches <- catMaybes <$> (forM matches $ \m -> do
        Right matched <- hvals $ C8.pack $ mName' ++ ":" ++ (C8.unpack m)
        let matchFields = map (T.unpack . E.decodeUtf8) matched
        liftIO $ putStrLn $ "  Testing match: " ++ show matchFields
        return $ if NGram.matchPrecise matchFields term'
            then Just m
            else Nothing)
    return [preciseMatches]
