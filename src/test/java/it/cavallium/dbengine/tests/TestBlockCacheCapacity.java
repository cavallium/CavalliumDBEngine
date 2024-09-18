package it.cavallium.dbengine.tests;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import it.cavallium.datagen.nativedata.Nullablelong;
import it.cavallium.dbengine.database.RocksDBLongProperty;
import it.cavallium.dbengine.database.disk.LLLocalKeyValueDatabase;
import it.cavallium.dbengine.rpc.current.data.Column;
import it.cavallium.dbengine.rpc.current.data.DatabaseOptions;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rocksdb.ColumnFamilyHandle;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletionException;

import static it.cavallium.dbengine.client.DefaultDatabaseOptions.DEFAULT_DATABASE_OPTIONS;
import static it.cavallium.dbengine.tests.DbTestUtils.ensureNoLeaks;

public class TestBlockCacheCapacity {

	private long cacheSmallSize = 64 * 1024 * 1024; // 64MiB
	private @NotNull DatabaseOptions opts;
	private Path tmpDir;
	private LLLocalKeyValueDatabase db;
	private ArrayList<ColumnFamilyHandle> handles;

	@BeforeEach
	public void setUp() throws IOException {
		System.setProperty("it.cavallium.dbengine.log.levelcode", "2");
		System.setProperty("rocksdb.debugging.useplaintablewheninmemory", "false");
		tmpDir = Files.createTempDirectory("rocksdb");
		opts = DEFAULT_DATABASE_OPTIONS.setBlockCache(Nullablelong.of(cacheSmallSize));
	}

	@AfterEach
	public void tearDown() throws IOException {
		try {
			db.close();
		} catch (Throwable ex) {
			ex.printStackTrace();
		}
		deleteDirectory(tmpDir);
	}

	private void startDb() {
		var cols = new ArrayList<Column>();
		cols.add(new Column("main"));
		handles = new ArrayList<ColumnFamilyHandle>();
		db = new LLLocalKeyValueDatabase(new SimpleMeterRegistry(), "test", true, tmpDir, cols, handles, opts);
	}

	@Test
	public void testBlockCacheCapeacity() throws IOException {
		startDb();
		testConditions(1.0f, 1.5f);
	}

	@Test
	public void testSpinningBlockCacheCapeacity() throws IOException {
		opts.setSpinning(true);
		startDb();
		testConditions(1.0f, 1.5f);
	}

	@Test
	public void testLRUCacheCapacity() throws IOException {
		System.setProperty("it.cavallium.dbengine.clockcache.enable", "false");
		startDb();
		testConditions(1.0f, 1.5f);
	}

	@Test
	public void testHyperClockCacheCapacity() throws IOException {
		System.setProperty("it.cavallium.dbengine.clockcache.enable", "true");
		startDb();
		testConditions(1.0f, 1.5f);
	}

	private void testConditions(float min, float max) {
		var property = db.getAggregatedLongProperty(RocksDBLongProperty.BLOCK_CACHE_CAPACITY);
		System.out.println("expected block cache capacity: " + cacheSmallSize / 1024 / 1024 + "MiB");
		System.out.println("block cache capacity: " + property / 1024 / 1024 + "MiB");
		var err = "Cache is not correct: %.1f != %.1f".formatted(property / 1024 / 1024d, cacheSmallSize / 1024 / 1024d);
		Assertions.assertTrue(property <= cacheSmallSize * max, err);
		Assertions.assertTrue(property >= cacheSmallSize * min, err);
	}

	private static void deleteDirectory(Path path) throws IOException {
		if (Files.exists(path)) {
			try (var walk = Files.walk(path)) {
				walk.sorted(Comparator.reverseOrder()).forEach(file -> {
					try {
						Files.delete(file);
					} catch (IOException ex) {
						throw new CompletionException(ex);
					}
				});
			}
		}
	}
}
