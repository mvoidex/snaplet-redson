{-# LANGUAGE OverloadedStrings #-}

module Snap.Snaplet.Redson.Search.NGram
    ( Index
    , searched
    , create
    , update
    , delete
    , getRecord
    , modifyIndex
    , initializeIndex
    ) where

import Control.Arrow

import Control.Monad.IO.Class

import Control.Concurrent.MVar

import qualified Data.Map as M
import Data.Maybe (mapMaybe, isJust, fromJust)
import Data.Monoid

import qualified Data.ByteString as B

import qualified Data.Text as T
import qualified Data.Text.Encoding as E

import Database.Redis

import qualified Text.Search.NGram as NG

import Snap.Snaplet.Redson.Snapless.CRUD (InstanceId)
import Snap.Snaplet.Redson.Snapless.Metamodel

-- | Decode from UTF-8
decodeS :: B.ByteString -> String
decodeS = T.unpack . E.decodeUtf8

-- | Encode to UTF-8
encodeS :: String -> B.ByteString
encodeS = E.encodeUtf8 . T.pack

-- | Index for one record
record :: [FieldIndex] -> Commit -> NG.IndexValue
record fs c = concatMap (NG.ngram 3) values where
    values = map decodeS $ mapMaybe (`M.lookup` c) (map fst fs)

-- | Index data
data Index a = Uninitialized (NG.IndexAction a) | Initialized (NG.Index a)

instance Ord a => Monoid (Index a) where
    mempty = Uninitialized mempty
    (Uninitialized l) `mappend` (Uninitialized r) = Uninitialized (l `mappend` r)
    (Uninitialized l) `mappend` (Initialized r) =
        Uninitialized $ l `mappend` NG.insertAction r
    (Initialized l) `mappend` (Uninitialized r) =
        Uninitialized $ NG.insertAction l `mappend` r
    (Initialized l) `mappend` (Initialized r) = Initialized (l `mappend` r)

-- | Apply index action
-- When index is initializing, new actions must append at left as new.
-- Initialized index may be changed directry
apply :: Ord a => NG.IndexAction a -> Index a -> Index a
apply act (Uninitialized f) = Uninitialized (act `mappend` f) -- act . f
apply act (Initialized v) = Initialized $ NG.apply act v -- act v

-- | Initialize index
initialize :: Ord a => NG.Index a -> Index a -> Index a
initialize i (Uninitialized v) = Initialized $ NG.apply v i
initialize i (Initialized v) = Initialized $ v `mappend` i

-- | Search subpart
searched :: Ord a => Index a -> NG.Index a
searched (Uninitialized v) = NG.apply v mempty
searched (Initialized v) = v

-- | Create index for new record and update indices with it
create
    :: InstanceId
    -> [FieldIndex]
    -> Commit
    -> NG.IndexAction InstanceId
create i fs c = NG.insert (record fs) c i

-- | Update existing record
update
    :: InstanceId
    -> [FieldIndex]
    -> Commit
    -> Commit
    -> NG.IndexAction InstanceId
update i fs c c' = NG.update (record fs) v c' i where
    v = M.intersectionWith const c c' -- remove only overlapped keys

-- | Remove record
delete
    :: InstanceId
    -> [FieldIndex]
    -> Commit
    -> NG.IndexAction InstanceId
delete i fs c = NG.remove (record fs) c i

-- | Key for record
recordKey :: ModelName -> InstanceId -> B.ByteString
recordKey mname i = B.concat [mname, ":", i]

-- | Get data by id
getRecord
    :: ModelName
    -> InstanceId
    -> [FieldIndex]
    -> Redis (Either Reply Commit)
getRecord mname i fs =
    let
        key = recordKey mname i
        fs' = map fst fs
    in do
        values <- hmget key fs'
        let toCommit v =
                M.fromList
                $ map (second fromJust)
                $ filter (isJust . snd)
                $ (fs' `zip` v)
        return $ fmap toCommit values

-- | Modify index with action
modifyIndex
    :: MVar (Index InstanceId)
    -> (NG.IndexAction InstanceId)
    -> Redis ()
modifyIndex mvar act = liftIO $ modifyMVar_ mvar (return . apply act)

-- | Initialize index
initializeIndex
    :: MVar (Index InstanceId)
    -> (NG.Index InstanceId)
    -> Redis ()
initializeIndex mvar ix = liftIO $ modifyMVar_ mvar (return . initialize ix)
