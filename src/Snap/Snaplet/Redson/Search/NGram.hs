module Snap.Snaplet.Redson.Search.NGram (
	Index(..),
	NGramConfig(..),
	empty,
	mergeUnion,
	mergeUnions,
	indexOne,
	indexList,
	search
	) where

-- Local
-- Libraries
-- Standard
import Control.Arrow
import Control.Applicative hiding (empty)
import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as C8
import qualified Data.IntMap as IM
import qualified Data.Map as M
import Data.Maybe
import Data.List
import qualified Data.Set as S

-- Declaration

-- | Index type
data Index key = Index {
	indexData :: IM.IntMap (S.Set key) }
		deriving (Eq)

-- | Config for N-Gram, empty for now
data NGramConfig = NGramConfig

-- | emptyIndex
empty :: Index key

-- | Merge indices
mergeUnion :: Ord key => Index key -> Index key -> Index key

-- | Merge list of indices
mergeUnions :: Ord key => [Index key] -> Index key

-- | Index one record
indexOne :: Ord key => NGramConfig -> ByteString -> key -> Index key

-- | Index list of data
indexList :: Ord key => NGramConfig -> [(ByteString, key)] -> Index key

-- | Search
search :: Ord key => Int -> Index key -> ByteString -> [(key, Int)]

-- Implementation

empty = Index IM.empty

mergeUnion (Index i1) (Index i2) = Index $ IM.unionWith S.union i1 i2

mergeUnions = Index . IM.unionsWith S.union . map indexData

indexOne cfg txt k = Index $ IM.unionsWith S.union $ IM.singleton <$> searchPattern 3 txt <*> pure (S.singleton k)

indexList cfg = mergeUnions . map (uncurry (indexOne cfg))

search maxError (Index ix) str =
	filter ((>= length pat - maxError) . snd)
	$ M.toList
	$ M.unionsWith (+)
	$ map fromSet
	$ mapMaybe (`IM.lookup` ix) pat
	where
		pat = searchPattern 3 str
		fromSet = M.fromAscList . (`zip` repeat 1) . S.toAscList

grams :: Int -> ByteString -> [Int]
grams n = filter (>= 256 ^ (n - 1)) . map (str2int . B.take n) . B.tails where
	str2int :: ByteString -> Int
	str2int = foldl' (+) 0 . zipWith (\i x -> fromIntegral x * 256 ^ i) [0..] . B.unpack

searchPattern :: Int -> ByteString -> [Int]
searchPattern n = concatMap (grams n) . C8.words

