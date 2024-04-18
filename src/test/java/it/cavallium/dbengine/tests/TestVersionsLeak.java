package it.cavallium.dbengine.tests;

import static it.cavallium.dbengine.client.DefaultDatabaseOptions.DEFAULT_DATABASE_OPTIONS;

import com.google.common.primitives.Longs;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import it.cavallium.buffer.Buf;
import it.cavallium.dbengine.database.ColumnUtils;
import it.cavallium.dbengine.database.LLDictionaryResultType;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLSnapshot;
import it.cavallium.dbengine.database.RocksDBLongProperty;
import it.cavallium.dbengine.database.UpdateMode;
import it.cavallium.dbengine.database.UpdateReturnMode;
import it.cavallium.dbengine.database.disk.LLLocalKeyValueDatabase;
import it.cavallium.dbengine.rpc.current.data.Column;
import it.cavallium.dbengine.rpc.current.data.DatabaseOptions;
import it.cavallium.dbengine.utils.StreamUtils;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Stream;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.FlushOptions;
import org.rocksdb.RocksDBException;

public class TestVersionsLeak {

	public static void main(String[] args) throws IOException, RocksDBException {
		Path dir = args.length > 0 ? Path.of(args[0]) : Files.createTempDirectory("rocksdb-");
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			if (Files.exists(dir)) {
				deleteDir(dir);
			}
		}));
		try {
			System.out.println("Dir: " + dir.toAbsolutePath());
			var columns = new ArrayList<Column>();
			columns.add(ColumnUtils.dictionary("test"));
			var handles = new ArrayList<ColumnFamilyHandle>();
			try (var kvdb = new LLLocalKeyValueDatabase(new SimpleMeterRegistry(),
					"test",
					false,
					dir.resolve("data"),
					columns,
					handles,
					DEFAULT_DATABASE_OPTIONS
			)) {
				var dict = kvdb.getDictionary("test", UpdateMode.ALLOW);
				long key = 0;
				byte[] keyBytes = new byte[8];
				var smallVal = Buf.createZeroes(128);
				smallVal.set(0, (byte) 70);
				smallVal.set(1, (byte) -5);
				smallVal.set(2, (byte) 20);
				smallVal.set(3, (byte) 15);
				smallVal.set(127, (byte) 4);
				var bigVal = Buf.createZeroes(30_000);
				bigVal.set(0, (byte) 51);
				bigVal.set(29_998, (byte) 111);
				bigVal.set(29_999, (byte) 4);
				for (int i = 0; i < 1_000_000_000; i++) {
					Buf val = i % 10_000 == 0 ? bigVal : smallVal;
					var keyF = key;
					toByteArray(key, keyBytes);

					StreamUtils.collectOn(ForkJoinPool.commonPool(),
							Stream.of(1, 2, 3, 4).parallel(),
							StreamUtils.executing(x -> {
								dict.put(Buf.wrap(keyBytes), val, LLDictionaryResultType.PREVIOUS_VALUE);
								dict.put(Buf.wrap(keyBytes), val, LLDictionaryResultType.PREVIOUS_VALUE_EXISTENCE);
								dict.put(Buf.wrap(keyBytes), val, LLDictionaryResultType.VOID);
								dict.updateAndGetDelta(Buf.wrap(keyBytes), prev -> val);
								dict.update(Buf.wrap(keyBytes), prev -> val, UpdateReturnMode.GET_NEW_VALUE);
								dict.update(Buf.wrap(keyBytes), prev -> val, UpdateReturnMode.GET_OLD_VALUE);
								dict.update(Buf.wrap(keyBytes), prev -> val, UpdateReturnMode.NOTHING);
								dict.isRangeEmpty(null, LLRange.of(Buf.wrap(Longs.toByteArray(keyF - 100)), Buf.wrap(Longs.toByteArray(keyF - 50))), true);
							})
					);

					if (i % 100_000 == 0) {
						System.out.printf("Progress: %d\tLive versions: %d\tVersions: %d%n",
								i,
								kvdb.getMemoryStats().liveVersions(),
								kvdb.getAggregatedLongProperty(RocksDBLongProperty.CURRENT_SUPER_VERSION_NUMBER)
						);
//							dict.sizeRange(null, LLRange.all(), false);
//							dict.sizeRange(null, LLRange.all(), true);
//							dict.sizeRange(null, LLRange.from(Buf.wrap(Longs.toByteArray(key - 100))), true);
//							dict.sizeRange(null, LLRange.from(Buf.wrap(Longs.toByteArray(key - 100))), false);
//							dict.sizeRange(null, LLRange.to(Buf.wrap(Longs.toByteArray(key - 100))), true);
//							dict.sizeRange(null, LLRange.to(Buf.wrap(Longs.toByteArray(key - 100))), false);
//							dict.sizeRange(null, LLRange.of(Buf.wrap(Longs.toByteArray(key - 100)), Buf.wrap(Longs.toByteArray(key - 50))), true);
//							dict.sizeRange(null, LLRange.of(Buf.wrap(Longs.toByteArray(key - 100)), Buf.wrap(Longs.toByteArray(key - 50))), false);

						StreamUtils.collect(dict.getRangeGrouped(null, LLRange.all(), 6, false), StreamUtils.executing(x -> {}));
						kvdb.flush();
					}

					key++;
				}
			}
		} finally {
			if (Files.exists(dir)) {
				deleteDir(dir);
			}
		}
	}

	private static void deleteDir(Path dir) {
		try (var p = Files.walk(dir)) {
			p.sorted(Comparator.reverseOrder()).forEach(TestVersionsLeak::deleteFile);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private static void toByteArray(long value, byte[] result) {
		for (int i = 7; i >= 0; i--) {
			result[i] = (byte) (value & 0xffL);
			value >>= 8;
		}
	}

	private static void deleteFile(Path path) {
		try {
			Files.deleteIfExists(path);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
