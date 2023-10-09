package it.cavallium.dbengine.database.disk;

import static it.cavallium.dbengine.utils.StreamUtils.resourceStream;
import static java.util.Objects.requireNonNull;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.LiveFileMetaData;
import org.rocksdb.RocksDB;
import org.rocksdb.SstFileMetaData;

public class RocksDBColumnFile extends RocksDBFile {

	private final ColumnFamilyHandle cfh;

	public RocksDBColumnFile(RocksDB db, ColumnFamilyHandle cfh, RocksDBFileMetadata metadata) {
		super(metadata);
		this.cfh = cfh;
	}

	public <T extends RocksDB> RocksDBColumnFile(T db, ColumnFamilyHandle cfh, LiveFileMetaData file) {
		this(db,
				cfh,
				new RocksDBFileMetadata(Path.of(file.path() + file.fileName()),
						file.fileName(),
						file.level(),
						new String(file.columnFamilyName(), StandardCharsets.UTF_8),
						file.numEntries(),
						file.size(),
						decodeRange(file.smallestKey(), file.largestKey())
				)
		);
	}

	public <T extends RocksDB> RocksDBColumnFile(T db, ColumnFamilyHandle cfh,
			SstFileMetaData file, byte[] columnFamilyName, int level) {
		this(db,
				cfh,
				new RocksDBFileMetadata(Path.of(file.path()+ file.fileName()),
						file.fileName(),
						level,
						new String(columnFamilyName, StandardCharsets.UTF_8),
						file.numEntries(),
						file.size(),
						decodeRange(file.smallestKey(), file.largestKey())
				)
		);
	}


}
