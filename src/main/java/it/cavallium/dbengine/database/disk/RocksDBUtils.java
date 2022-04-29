package it.cavallium.dbengine.database.disk;

import static com.google.common.collect.Lists.partition;

import it.cavallium.dbengine.rpc.current.data.Column;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.Logger;
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

	public static int getLastLevel(RocksDB db, ColumnFamilyHandle cfh) {
		var lastLevel = db.numberLevels(cfh);
		if (lastLevel == 0) {
			return 6;
		} else {
			return lastLevel;
		}
	}

	public static List<String> getColumnFiles(RocksDB db, ColumnFamilyHandle cfh, boolean excludeLastLevel) {
		List<String> files = new ArrayList<>();
		var meta = db.getColumnFamilyMetaData(cfh);
		var lastLevel = excludeLastLevel ? getLastLevel(db, cfh) : -1;
		for (LevelMetaData level : meta.levels()) {
			if (!excludeLastLevel || level.level() < lastLevel) {
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
				int finalBottommostLevel = getLastLevel(db, cfh);
				Mono.whenDelayError(partitions.stream().map(partition -> Mono.<Void>fromCallable(() -> {
					logger.info("Compacting {} files in database {} in column family {} to level {}",
							partition.size(),
							logDbName,
							new String(cfh.getName(), StandardCharsets.UTF_8),
							finalBottommostLevel
					);
					if (!partition.isEmpty()) {
						var coi = new CompactionJobInfo();
						try {
							db.compactFiles(co, cfh, partition, finalBottommostLevel, volumeId, coi);
							logger.info("Compacted {} files in database {} in column family {} to level {}: {}",
									partition.size(),
									logDbName,
									new String(cfh.getName(), StandardCharsets.UTF_8),
									finalBottommostLevel,
									coi.status().getCodeString()
							);
						} catch (Throwable ex) {
							logger.error("Failed to compact {} files in database {} in column family {} to level {}",
									partition.size(),
									logDbName,
									new String(cfh.getName(), StandardCharsets.UTF_8),
									finalBottommostLevel,
									ex
							);
						}
					}
					return null;
				}).subscribeOn(Schedulers.boundedElastic())).toList()).block();
			}
		}
	}
}
