package it.cavallium.dbengine.database.disk;

import static com.google.common.collect.Lists.partition;

import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.rpc.current.data.Column;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.AbstractImmutableNativeReference;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.CompactionJobInfo;
import org.rocksdb.CompactionOptions;
import org.rocksdb.CompressionType;
import org.rocksdb.LevelMetaData;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileMetaData;
import org.rocksdb.util.SizeUnit;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class RocksDBUtils {

	public static int getLevels(RocksDB db, ColumnFamilyHandle cfh) {
		return db.numberLevels(cfh);
	}

	public static List<String> getColumnFiles(RocksDB db, ColumnFamilyHandle cfh, boolean excludeLastLevel) {
		List<String> files = new ArrayList<>();
		var meta = db.getColumnFamilyMetaData(cfh);
		var lastLevelId = excludeLastLevel ? (getLevels(db, cfh) - 1) : -1;
		for (LevelMetaData level : meta.levels()) {
			if (!excludeLastLevel || level.level() < lastLevelId) {
				for (SstFileMetaData file : level.files()) {
					if (file.fileName().endsWith(".sst")) {
						files.add(file.fileName());
					}
				}
			}
		}
		return files;
	}

	public static void forceCompaction(RocksDB db,
			String logDbName,
			ColumnFamilyHandle cfh,
			int volumeId,
			Logger logger) {
		try (var co = new CompactionOptions()
				.setCompression(CompressionType.LZ4_COMPRESSION)
				.setMaxSubcompactions(0)
				.setOutputFileSizeLimit(2 * SizeUnit.GB)) {
			List<String> filesToCompact = getColumnFiles(db, cfh, true);

			if (!filesToCompact.isEmpty()) {
				var partitionSize = filesToCompact.size() / Runtime.getRuntime().availableProcessors();
				List<List<String>> partitions;
				if (partitionSize > 0) {
					partitions = partition(filesToCompact, partitionSize);
				} else {
					partitions = List.of(filesToCompact);
				}
				int finalBottommostLevelId = getLevels(db, cfh) - 1;
				Mono.whenDelayError(partitions.stream().map(partition -> Mono.<Void>fromCallable(() -> {
					logger.info("Compacting {} files in database {} in column family {} to level {}",
							partition.size(),
							logDbName,
							new String(cfh.getName(), StandardCharsets.UTF_8),
							finalBottommostLevelId
					);
					if (!partition.isEmpty()) {
						var coi = new CompactionJobInfo();
						try {
							db.compactFiles(co, cfh, partition, finalBottommostLevelId, volumeId, coi);
							logger.info("Compacted {} files in database {} in column family {} to level {}: {}",
									partition.size(),
									logDbName,
									new String(cfh.getName(), StandardCharsets.UTF_8),
									finalBottommostLevelId,
									coi.status().getCodeString()
							);
						} catch (Throwable ex) {
							logger.error("Failed to compact {} files in database {} in column family {} to level {}",
									partition.size(),
									logDbName,
									new String(cfh.getName(), StandardCharsets.UTF_8),
									finalBottommostLevelId,
									ex
							);
						}
					}
					return null;
				}).subscribeOn(Schedulers.boundedElastic())).toList()).transform(LLUtils::handleDiscard).block();
			}
		}
	}

	public static void ensureOpen(RocksDB db, @Nullable ColumnFamilyHandle cfh) {
		if (Schedulers.isInNonBlockingThread()) {
			throw new UnsupportedOperationException("Called in a nonblocking thread");
		}
		ensureOwned(db);
		ensureOwned(cfh);
	}

	public static void ensureOwned(@Nullable AbstractImmutableNativeReference rocksObject) {
		if (rocksObject != null && !rocksObject.isOwningHandle()) {
			throw new IllegalStateException("Not owning handle");
		}
	}
}
