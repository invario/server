<?php

namespace OC\Files\Cache;

use OCP\DB\QueryBuilder\IQueryBuilder;

/**
 * Lower level access to the file cache
 */
class CacheAccess {
	public function __construct(
		private CacheDatabase $cacheDatabase
	) {
	}

	public function getByFileIdInStorage(int $fileId, int $storageId): ?CacheEntry {
		$entry = $this->queryByFileIdInShard($fileId, $this->cacheDatabase->getShardForStorageId($storageId));
		if ($entry) {
			$this->cacheDatabase->setCachedStorageIdForFileId($fileId, $storageId);
		}
		return $entry;
	}

	public function getByPathInStorage(string $path, int $storageId): ?CacheEntry {
		$query = $this->cacheDatabase->queryForShard($this->cacheDatabase->getShardForStorageId($storageId))->selectFileCache();
		$query->andWhere($query->expr()->eq('filecache.path_hash', $query->createNamedParameter(md5($path))));

		$row = $query->executeQuery()->fetch();
		if ($row) {
			$entry = new CacheEntry($row);
			$this->cacheDatabase->setCachedStorageIdForFileId($entry->getId(), $storageId);
			return $entry;
		} else {
			return null;
		}
	}

	public function getByFileId(int $fileId): ?CacheEntry {
		$cachedStorage = $this->cacheDatabase->getCachedStorageIdForFileId($fileId);
		if ($cachedStorage) {
			$result = $this->queryByFileIdInShard($fileId, $this->cacheDatabase->getShardForStorageId($cachedStorage));
			if ($result && $result->getId() === $fileId) {
				return $result;
			}
		}

		foreach ($this->cacheDatabase->getAllShards() as $shard) {
			$result = $this->queryByFileIdInShard($fileId, $shard);
			if ($result) {
				$this->cacheDatabase->setCachedStorageIdForFileId($fileId, $result->getStorageId());
				return $result;
			}
		}
		return null;
	}

	private function queryByFileIdInShard(int $fileId, int $shard): ?CacheEntry {
		$query = $this->cacheDatabase->queryForShard($shard)->selectFileCache();
		$query->andWhere($query->expr()->eq('filecache.fileid', $query->createNamedParameter($fileId, IQueryBuilder::PARAM_INT)));

		$row = $query->executeQuery()->fetch();
		return $row ? new CacheEntry($row) : null;
	}

	/**
	 * @param list<int> $fileIds
	 * @return array<int, CacheEntry>
	 */
	public function getByFileIds(array $fileIds): array {
		$cachedStorages = $this->cacheDatabase->getCachedShardsForFileIds($fileIds);

		$foundItems = [];
		foreach ($cachedStorages as $shard => $fileIdsForShard) {
			$foundItems += $this->queryByFileIdsInShard($shard, $fileIdsForShard);
		}

		$remainingIds = array_diff($fileIds, array_keys($foundItems));

		if ($remainingIds) {
			foreach ($this->cacheDatabase->getAllShards() as $shard) {
				$items = $this->queryByFileIdsInShard($shard, $remainingIds);
				foreach ($items as $item) {
					$this->cacheDatabase->setCachedStorageIdForFileId($item->getId(), $item->getStorageId());
				}

				$remainingIds = array_diff($remainingIds, array_keys($items));
				$foundItems += $items;

				if (count($remainingIds) === 0) {
					break;
				}
			}
		}
		return $foundItems;
	}

	/**
	 * @param list<int> $fileIds
	 * @return array<int, CacheEntry>
	 */
	public function queryByFileIdsInShard(int $shard, array $fileIds): array {
		$query = $this->cacheDatabase->queryForShard($shard)->selectFileCache();
		$query->andWhere($query->expr()->in('filecache.fileid', $query->createNamedParameter($fileIds, IQueryBuilder::PARAM_INT_ARRAY)));
		$result = $query->executeQuery();
		$items = [];
		while ($row = $result->fetch()) {
			$items[(int)$row['fileid']] = new CacheEntry($row);
		}
		return $items;
	}

	/**
	 * @param list<int> $fileIds
	 * @param int $storageId
	 * @return array<int, CacheEntry>
	 */
	public function getByFileIdsInStorage(array $fileIds, int $storageId): array {
		$query = $this->getQuery()->selectFileCache();
		$query->andWhere($query->expr()->in('filecache.fileid', $query->createNamedParameter($fileIds, IQueryBuilder::PARAM_INT_ARRAY)));
		$query->andWhere($query->expr()->eq('filecache.storage', $query->createNamedParameter($storageId, IQueryBuilder::PARAM_INT)));

		$rows = $query->executeQuery()->fetchAll();
		return array_map(function (array $row) {
			return new CacheEntry($row);
		}, $rows);
	}
}
