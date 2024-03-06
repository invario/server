<?php

namespace OC\Files\Cache;

use OC\FilesMetadata\FilesMetadataManager;
use OC\SystemConfig;
use OCP\DB\QueryBuilder\IQueryBuilder;
use OCP\IDBConnection;
use Psr\Log\LoggerInterface;

/**
 * Lower level access to the file cache
 */
class CacheAccess {
	public function __construct(
		private IDBConnection $connection,
		private SystemConfig $systemConfig,
		private LoggerInterface $logger,
		private FilesMetadataManager $metadataManager
	) {
	}

	private function getQuery(): CacheQueryBuilder {
		return new CacheQueryBuilder(
			$this->connection,
			$this->systemConfig,
			$this->logger,
			$this->metadataManager,
		);
	}

	public function getByFileIdInStorage(int $fileId, int $storageId): ?CacheEntry {
		$query = $this->getQuery()->selectFileCache();
		$query->andWhere($query->expr()->eq('filecache.fileid', $query->createNamedParameter($fileId, IQueryBuilder::PARAM_INT)));
		$query->andWhere($query->expr()->eq('filecache.storage', $query->createNamedParameter($storageId, IQueryBuilder::PARAM_INT)));

		$row = $query->executeQuery()->fetch();
		return $row ? new CacheEntry($row) : null;
	}

	public function getByPathInStorage(string $path, int $storageId): ?CacheEntry {
		$query = $this->getQuery()->selectFileCache();
		$query->andWhere($query->expr()->eq('filecache.path_hash', $query->createNamedParameter(md5($path))));

		$row = $query->executeQuery()->fetch();
		return $row ? new CacheEntry($row) : null;
	}

	public function getByFileId(int $fileId): ?CacheEntry {
		$query = $this->getQuery()->selectFileCache();
		$query->andWhere($query->expr()->eq('filecache.fileid', $query->createNamedParameter($fileId, IQueryBuilder::PARAM_INT)));

		$row = $query->executeQuery()->fetch();
		return $row ? new CacheEntry($row) : null;
	}

	/**
	 * @param list<int> $fileIds
	 * @return array<int, CacheEntry>
	 */
	public function getByFileIds(array $fileIds): array {
		$query = $this->getQuery()->selectFileCache();
		$query->andWhere($query->expr()->in('filecache.fileid', $query->createNamedParameter($fileIds, IQueryBuilder::PARAM_INT_ARRAY)));

		$rows = $query->executeQuery()->fetchAll();
		return array_map(function (array $row) {
			return new CacheEntry($row);
		}, $rows);
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
