{-# LANGUAGE OverloadedStrings #-}

module Snap.Snaplet.Redson.Search.NGram
    ( create
    , update
    , delete
    , getRecord
    , modifyIndex
    ) where

import Control.Arrow

import Control.Monad.IO.Class

import Control.Concurrent.MVar

import qualified Data.Map as M
import Data.Maybe (mapMaybe, isJust, fromJust)

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

-- | Create index for new record and update indices with it
create
    :: InstanceId
    -> [FieldIndex]
    -> Commit
    -> (NG.Index InstanceId -> NG.Index InstanceId)
create i fs c = NG.apply (NG.insert (record fs) c i)

-- | Update existing record
update
    :: InstanceId
    -> [FieldIndex]
    -> Commit
    -> Commit
    -> (NG.Index InstanceId -> NG.Index InstanceId)
update i fs c c' = NG.apply (NG.update (record fs) v c' i) where
    v = M.intersectionWith const c c' -- remove only overlapped keys

-- | Remove record
delete
    :: InstanceId
    -> [FieldIndex]
    -> Commit
    -> (NG.Index InstanceId -> NG.Index InstanceId)
delete i fs c = NG.apply (NG.remove (record fs) c i)

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
    :: MVar (NG.Index InstanceId)
    -> (NG.Index InstanceId -> NG.Index InstanceId)
    -> Redis ()
modifyIndex mvar act = liftIO $ modifyMVar_ mvar (return . act)
