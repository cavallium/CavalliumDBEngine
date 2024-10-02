package it.cavallium.dbengine.database.disk;

import static it.cavallium.dbengine.utils.StreamUtils.resourceStream;
import static java.util.Objects.requireNonNull;

import com.google.common.primitives.Longs;
import it.cavallium.buffer.Buf;
import it.cavallium.dbengine.client.SSTDumpProgress;
import it.cavallium.dbengine.client.SSTDumpProgress.SSTBlockFail;
import it.cavallium.dbengine.client.SSTDumpProgress.SSTBlockKeyValue;
import it.cavallium.dbengine.client.SSTProgress.SSTOk;
import it.cavallium.dbengine.client.SSTProgress.SSTProgressReport;
import it.cavallium.dbengine.client.SSTProgress.SSTStart;
import it.cavallium.dbengine.client.SSTVerificationProgress;
import it.cavallium.dbengine.client.SSTVerificationProgress.SSTBlockBad;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.RocksDBFile.RocksDBFileIterationKeyState.RocksDBFileIterationStateKeyError;
import it.cavallium.dbengine.database.disk.RocksDBFile.RocksDBFileIterationKeyState.RocksDBFileIterationStateKeyOk;
import it.cavallium.dbengine.database.disk.RocksDBFile.RocksDBFileIterationState.RocksDBFileIterationStateBegin;
import it.cavallium.dbengine.database.disk.RocksDBFile.RocksDBFileIterationState.RocksDBFileIterationStateEnd;
import it.cavallium.dbengine.database.disk.RocksDBFile.RocksDBFileIterationState.RocksDBFileIterationStateKey;
import it.cavallium.dbengine.database.disk.SSTRange.SSTLLRange;
import it.cavallium.dbengine.database.disk.SSTRange.SSTRangeFull;
import it.cavallium.dbengine.database.disk.SSTRange.SSTRangeKey;
import it.cavallium.dbengine.database.disk.SSTRange.SSTRangeNone;
import it.cavallium.dbengine.database.disk.SSTRange.SSTRangeOffset;
import it.cavallium.dbengine.database.disk.SSTRange.SSTSingleKey;
import it.cavallium.dbengine.database.disk.rocksdb.LLSstFileReader;
import it.cavallium.dbengine.utils.StreamUtils;
import java.nio.file.Path;
import java.util.StringJoiner;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class RocksDBFile implements Comparable<RocksDBFile> {

	protected static final Logger logger = LogManager.getLogger(RocksDBFile.class);
	protected final RocksDBFileMetadata metadata;
	protected final Long sstNumber;

	public RocksDBFile(RocksDBFileMetadata metadata) {
		this.metadata = metadata;
		String fileName = metadata.fileName().startsWith("/") ? metadata.fileName().substring(1) : metadata.fileName();
		int extensionIndex = fileName.indexOf(".");
		Long sstNumber = null;
		if (extensionIndex != -1) {
			String numberRaw = fileName.substring(0, extensionIndex);
			this.sstNumber = Longs.tryParse(numberRaw);
		} else {
			this.sstNumber = null;
		}
	}

	public <T extends RocksDB> RocksDBFile(Path dbBaseDir, String file) {
		this(new RocksDBFileMetadata(dbBaseDir.resolve(file.startsWith("/") ? file.substring(1) : file),
						StringUtils.substringAfter(file, '/'),
						0,
						"any",
						0,
						0,
						LLRange.all()
				)
		);
	}

	protected static LLRange decodeRange(byte[] smallestKey, byte[] largestKey) {
		return LLRange.of(Buf.wrap(smallestKey), Buf.wrap(largestKey));
	}

	private static SSTRange intersectWithMetadata(LLRange metadataRange, SSTRange innerRange) {
		return switch (innerRange) {
			case SSTRangeFull ignored -> SSTRange.parse(metadataRange);
			case SSTSingleKey singleKey -> SSTRange.parse(LLRange.intersect(metadataRange, singleKey.toLLRange()));
			case SSTRangeKey rangeKey -> SSTRange.parse(LLRange.intersect(metadataRange, rangeKey.toLLRange()));
			case SSTRangeNone none -> none;
			case SSTRangeOffset offset -> offset;
		};
	}

	public RocksDBFileMetadata getMetadata() {
		return metadata;
	}

	public Stream<SSTVerificationProgress> verify(SSTRange range) {
		AtomicLong fileScanned = new AtomicLong();
		AtomicLong fileTotal = new AtomicLong();
		return iterate(range, true).map(state -> switch (state) {
			case RocksDBFileIterationStateBegin begin -> {
				var countEstimate = begin.metadata().countEstimate();
				if (countEstimate != null) {
					fileTotal.set(countEstimate);
				}
				yield new SSTStart(begin.metadata());
			}
			case RocksDBFileIterationStateKey key -> {
				var scanned = fileScanned.incrementAndGet();
				yield switch (key.state()) {
					case RocksDBFileIterationStateKeyOk ignored ->
							new SSTProgressReport(scanned, Math.max(scanned, fileTotal.get()));
					case RocksDBFileIterationStateKeyError keyError -> new SSTBlockBad(key.key, keyError.exception);
				};
			}
			case RocksDBFileIterationStateEnd end -> new SSTOk(end.scannedCount());
		});
	}

	public Stream<SSTDumpProgress> readAllSST(SSTRange range, boolean failOnError, boolean disableRocksdbChecks) {
		AtomicLong fileScanned = new AtomicLong();
		AtomicLong fileTotal = new AtomicLong();
		return iterate(range, disableRocksdbChecks).<SSTDumpProgress>mapMulti((state, consumer) -> {
			switch (state) {
				case RocksDBFileIterationStateBegin begin -> {
					var countEstimate = begin.metadata().countEstimate();
					if (countEstimate != null) {
						fileTotal.set(countEstimate);
					}
					consumer.accept(new SSTStart(begin.metadata()));
				}
				case RocksDBFileIterationStateKey key -> {
					var scanned = fileScanned.incrementAndGet();
					switch (key.state()) {
						case RocksDBFileIterationStateKeyOk ignored -> {
							consumer.accept(new SSTBlockKeyValue(key.key(), ignored.value()));
							consumer.accept(new SSTProgressReport(scanned, Math.max(scanned, fileTotal.get())));
						}
						case RocksDBFileIterationStateKeyError keyError -> {
							if (failOnError) {
								throw new CompletionException(keyError.exception());
							} else {
								logger.error("Corrupted SST \"{}\" after \"{}\" scanned keys", sstNumber, scanned);
								// This is sent before bad block, so takewhile still returns ok before the end, if failOnError is false
								consumer.accept(new SSTOk(scanned));
							}
							consumer.accept(new SSTBlockFail(keyError.exception));
						}
					}
				}
				case RocksDBFileIterationStateEnd end -> consumer.accept(new SSTOk(end.scannedCount()));
			}
		}).takeWhile(data -> !(data instanceof SSTBlockFail));
	}

	public Stream<RocksDBFileIterationState> iterate(SSTRange rangeFull, boolean disableRocksdbChecks) {
		var intersectedRange = RocksDBFile.intersectWithMetadata(metadata.keysRange(), rangeFull);

		Path filePath = metadata.filePath();
		String filePathString = filePath.toString();
		var meta = new IterationMetadata(filePath,
				metadata.fileName().replace("/", ""),
				intersectedRange,
				metadata.numEntries() > 0 ? metadata.numEntries() : null,
				sstNumber
		);

		Stream<RocksDBFileIterationState> streamContent;
		// Ignore the file if it's outside the requested range
		if (intersectedRange instanceof SSTRangeNone) {
			streamContent = Stream.of(new RocksDBFileIterationStateBegin(meta), new RocksDBFileIterationStateEnd(0L));
		} else {
			AtomicLong fileScanned = new AtomicLong();
			AtomicBoolean mustSeek = new AtomicBoolean(true);
			try {
				streamContent = resourceStream(() -> new LLSstFileReader(!disableRocksdbChecks, filePathString),
						r -> resourceStream(() -> LLUtils.generateCustomReadOptions(null, false, intersectedRange.isBounded(), false),
								ro -> {
									long skipToIndex;
									long readToCount;
									switch (intersectedRange) {
										case SSTLLRange sstllRange -> {
											var llRange = sstllRange.toLLRange();
											requireNonNull(llRange);
											ro.setIterateLowerBound(
													llRange.getMin() != null ? requireNonNull(LLUtils.asArray(llRange.getMin())) : null);
											ro.setIterateUpperBound(
													llRange.getMax() != null ? requireNonNull(LLUtils.asArray(llRange.getMax())) : null);
											skipToIndex = 0;
											readToCount = Long.MAX_VALUE;
										}
										case SSTRangeOffset offset -> {
											skipToIndex = offset.offsetMin() == null ? 0 : offset.offsetMin();
											readToCount = offset.offsetMax() == null ? Long.MAX_VALUE : (offset.offsetMax() - skipToIndex);
										}
										default -> throw new IllegalStateException("Unexpected value: " + intersectedRange);
									}
									ro.setFillCache(true);
									ro.setIgnoreRangeDeletions(true);
									if (!(intersectedRange instanceof SSTSingleKey)) {
										ro.setReadaheadSize(256 * 1024 * 1024);
									}
									ro.setVerifyChecksums(true);
									return resourceStream(() -> ro.newIterator(r.get(), IteratorMetrics.NO_OP),
											rocksIterator -> StreamUtils.<RocksDBFileIterationState>streamUntil(() -> {
												boolean mustSeekVal = mustSeek.compareAndSet(true, false);
												if (!mustSeekVal && !rocksIterator.isValid()) {
													return new RocksDBFileIterationStateEnd(fileScanned.get());
												}
												Buf rawKey = null;
												Buf rawValue = null;
												RocksDBFileIterationKeyState keyResult;
												var index = fileScanned.getAndIncrement();
												if (index >= readToCount) {
													return new RocksDBFileIterationStateEnd(fileScanned.get());
												} else {
													try {
														if (mustSeekVal) {
															rocksIterator.seekToFirstUnsafe();
															if (skipToIndex > 0) {
																for (long i = 0; i < skipToIndex; i++) {
																	if (!rocksIterator.isValid()) {
																		break;
																	}
																	rocksIterator.nextUnsafe();
																}
															}
															return new RocksDBFileIterationStateBegin(meta);
														} else {
															rocksIterator.statusUnsafe();
															rawKey = rocksIterator.keyBuf().copy();
															rawValue = rocksIterator.valueBuf().copy();
															rocksIterator.next();
														}
														keyResult = new RocksDBFileIterationStateKeyOk(rawValue);
													} catch (RocksDBException ex) {
														keyResult = new RocksDBFileIterationStateKeyError(ex);
													}

													return new RocksDBFileIterationStateKey(rawKey, keyResult, index);
												}
											}, x -> x instanceof RocksDBFileIterationStateEnd).onClose(() -> {
												rocksIterator.close();
												ro.close();
											})
									);
								}
						)
				);
			} catch (RocksDBException e) {
				streamContent = Stream.of(new RocksDBFileIterationStateBegin(meta),
						new RocksDBFileIterationStateKey(null, new RocksDBFileIterationStateKeyError(e), 0));
			}
		}
		return streamContent;
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

	public Long getSstNumber() {
		return sstNumber;
	}

	public sealed interface RocksDBFileIterationState {

		record RocksDBFileIterationStateBegin(IterationMetadata metadata) implements RocksDBFileIterationState {}

		record RocksDBFileIterationStateKey(Buf key, RocksDBFileIterationKeyState state, long scannedCount) implements
				RocksDBFileIterationState {}

		record RocksDBFileIterationStateEnd(long scannedCount) implements RocksDBFileIterationState {}
	}

	public sealed interface RocksDBFileIterationKeyState {

		record RocksDBFileIterationStateKeyOk(Buf value) implements RocksDBFileIterationKeyState {}

		record RocksDBFileIterationStateKeyError(RocksDBException exception) implements RocksDBFileIterationKeyState {}
	}

	public record IterationMetadata(Path filePath, String filename, @NotNull SSTRange range,
																	@Nullable Long countEstimate, @Nullable Long sstNumber) {}
}
