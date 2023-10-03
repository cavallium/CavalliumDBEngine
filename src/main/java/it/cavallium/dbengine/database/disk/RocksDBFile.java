package it.cavallium.dbengine.database.disk;

import static it.cavallium.dbengine.database.LLUtils.isBoundedRange;
import static it.cavallium.dbengine.database.disk.LLLocalKeyValueDatabase.PRINT_ALL_CHECKSUM_VERIFICATION_STEPS;
import static it.cavallium.dbengine.utils.StreamUtils.resourceStream;
import static java.util.Objects.requireNonNull;

import com.google.common.primitives.Longs;
import it.cavallium.buffer.Buf;
import it.cavallium.dbengine.client.VerificationProgress;
import it.cavallium.dbengine.client.VerificationProgress.BlockBad;
import it.cavallium.dbengine.client.VerificationProgress.FileOk;
import it.cavallium.dbengine.client.VerificationProgress.FileStart;
import it.cavallium.dbengine.client.VerificationProgress.Progress;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.rocksdb.LLReadOptions;
import it.cavallium.dbengine.rpc.current.data.Column;
import it.cavallium.dbengine.utils.StreamUtils;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.LiveFileMetaData;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileMetaData;

public class RocksDBFile implements Comparable<RocksDBFile> {

	protected static final Logger logger = LogManager.getLogger(RocksDBFile.class);

	private final RocksDB db;
	private final ColumnFamilyHandle cfh;
	private final RocksDBFileMetadata metadata;
	private final Long sstNumber;

	public RocksDBFile(RocksDB db, ColumnFamilyHandle cfh, RocksDBFileMetadata metadata) {
		this.db = db;
		this.cfh = cfh;
		this.metadata = metadata;
		String fileName = metadata.fileName().replace("/", "");
		int extensionIndex = fileName.indexOf(".");
		Long sstNumber = null;
		if (extensionIndex != -1) {
			String numberRaw = fileName.substring(0, extensionIndex);
			//noinspection UnstableApiUsage
			this.sstNumber = Longs.tryParse(numberRaw);
		} else {
			this.sstNumber = null;
		}
	}

	public <T extends RocksDB> RocksDBFile(T db, ColumnFamilyHandle cfh, LiveFileMetaData file) {
		this(db,
				cfh,
				new RocksDBFileMetadata(file.path(),
						file.fileName(),
						file.level(),
						new String(file.columnFamilyName(), StandardCharsets.UTF_8),
						file.numEntries(),
						file.size(),
						decodeRange(file.smallestKey(), file.largestKey())
				)
		);
	}

	public <T extends RocksDB> RocksDBFile(T db, ColumnFamilyHandle cfh, SstFileMetaData file, byte[] columnFamilyName, int level) {
		this(db,
				cfh,
				new RocksDBFileMetadata(file.path(),
						file.fileName(),
						level,
						new String(columnFamilyName, StandardCharsets.UTF_8),
						file.numEntries(),
						file.size(),
						decodeRange(file.smallestKey(), file.largestKey())
				)
		);
	}

	private static LLRange decodeRange(byte[] smallestKey, byte[] largestKey) {
		return LLRange.of(Buf.wrap(smallestKey), Buf.wrap(largestKey));
	}

	public RocksDBFileMetadata getMetadata() {
		return metadata;
	}

	public record VerificationFileRange(String filePath, String filename, @Nullable LLRange range, long countEstimate,
																			@Nullable Long sstNumber) {}

	public Stream<VerificationProgress> verify(String databaseDisplayName, String columnDisplayName, LLRange rangeFull) {
		var intersectedRange = LLRange.intersect(metadata.keysRange(), rangeFull);
		// Ignore the file if it's outside the requested range
		if (intersectedRange == null) {
			return Stream.of();
		}

		String filePath = Path.of(metadata.path()).resolve("./" + metadata.fileName()).normalize().toString();
		var fr = new VerificationFileRange(filePath,
				metadata.fileName().replace("/", ""),
				intersectedRange,
				metadata.numEntries(),
				sstNumber
		);
		return verify(databaseDisplayName, columnDisplayName, rangeFull, fr);
	}

	private Stream<VerificationProgress> verify(String databaseDisplayName, String columnDisplayName, LLRange rangeFull, VerificationFileRange fr) {
		var columnObj = Column.of(columnDisplayName);
		Stream<VerificationProgress> streamInit = Stream.of(new FileStart(databaseDisplayName, columnObj, fr.filePath, fr.countEstimate > 0 ? fr.countEstimate : null));
		Stream<VerificationProgress> streamContent;
		Objects.requireNonNull(fr.range);

		String filename = fr.filename;
		String path = fr.filePath;
		LLRange rangePartial = fr.range;
		AtomicLong fileScanned = new AtomicLong();
		final long fileEstimate = fr.countEstimate;
		AtomicBoolean mustSeek = new AtomicBoolean(true);
		AtomicBoolean streamEnded = new AtomicBoolean(false);
		streamContent = resourceStream(
				() -> LLUtils.generateCustomReadOptions(null, false, isBoundedRange(rangePartial), false),
				ro -> {
					ro.setIterateLowerBound(rangePartial.getMin() != null ? requireNonNull(LLUtils.asArray(rangePartial.getMin())) : null);
					ro.setIterateUpperBound(rangePartial.getMax() != null ? requireNonNull(LLUtils.asArray(rangePartial.getMax())) : null);
					ro.setFillCache(false);
					ro.setIgnoreRangeDeletions(true);
					if (!rangePartial.isSingle()) {
						ro.setReadaheadSize(256 * 1024 * 1024);
					}
					ro.setVerifyChecksums(true);
					return resourceStream(() -> ro.newIterator(db, cfh, IteratorMetrics.NO_OP), rocksIterator -> StreamUtils
							.<Optional<VerificationProgress>>streamWhileNonNull(() -> {
								boolean mustSeekVal = mustSeek.compareAndSet(true, false);
								if (!mustSeekVal && !rocksIterator.isValid()) {
									if (streamEnded.compareAndSet(false, true)) {
										return Optional.of(new FileOk(databaseDisplayName, columnObj, path, fileScanned.get()));
									} else {
										//noinspection OptionalAssignedToNull
										return null;
									}
								}
								boolean shouldSendStatus;
								Buf rawKey = null;
								try {
									if (mustSeekVal) {
										if (PRINT_ALL_CHECKSUM_VERIFICATION_STEPS) {
											logger.info("Seeking to {}->{}->first on file {}", databaseDisplayName, columnDisplayName, filename);
										}
										rocksIterator.seekToFirst();
										shouldSendStatus = true;
									} else {
										rawKey = rocksIterator.keyBuf().copy();
										shouldSendStatus = fileScanned.incrementAndGet() % 1_000_000 == 0;
										if (PRINT_ALL_CHECKSUM_VERIFICATION_STEPS) {
											logger.info("Checking {}->{}->{} on file {}", databaseDisplayName, columnDisplayName, rawKey.toString(), filename);
										}
										rocksIterator.next();
									}
								} catch (RocksDBException ex) {
									return Optional.of(new BlockBad(databaseDisplayName, columnObj, rawKey, path, ex));
								}
								if (shouldSendStatus) {
									long fileScannedVal = fileScanned.get();
									return Optional.of(new Progress(databaseDisplayName,
											columnObj,
											path,
											-1,
											-1,
											fileScannedVal,
											Math.max(fileEstimate, fileScannedVal)
									));
								} else {
									return Optional.empty();
								}
							}).filter(Optional::isPresent).map(Optional::get).onClose(() -> {
								rocksIterator.close();
								ro.close();
							}));
				}
		);
		return Stream.concat(streamInit, streamContent);
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", RocksDBFile.class.getSimpleName() + "[", "]")
				.add("fileMetadata=" + metadata)
				.toString();
	}

	@Override
	public int compareTo(@NotNull RocksDBFile o) {
		if (this.sstNumber == null && o.sstNumber == null) {
			return 0;
		} else if (this.sstNumber == null) {
			return 1;
		} else if (o.sstNumber == null) {
			return -1;
		}
		return Long.compare(this.sstNumber, o.sstNumber);
	}
}
