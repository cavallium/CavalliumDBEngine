package it.cavallium.dbengine.repair;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.logging.LoggingMeterRegistry;
import it.cavallium.datagen.nativedata.NullableString;
import it.cavallium.datagen.nativedata.Nullableboolean;
import it.cavallium.datagen.nativedata.Nullableint;
import it.cavallium.datagen.nativedata.Nullablelong;
import it.cavallium.dbengine.client.VerificationProgress.BlockBad;
import it.cavallium.dbengine.client.VerificationProgress.FileOk;
import it.cavallium.dbengine.client.VerificationProgress.Progress;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.UpdateMode;
import it.cavallium.dbengine.database.disk.LLLocalDatabaseConnection;
import it.cavallium.dbengine.database.disk.LLLocalDictionary;
import it.cavallium.dbengine.database.disk.LLLocalKeyValueDatabase;
import it.cavallium.dbengine.rpc.current.data.Column;
import it.cavallium.dbengine.rpc.current.data.DatabaseOptionsBuilder;
import it.cavallium.dbengine.rpc.current.data.DatabaseVolume;
import it.cavallium.dbengine.rpc.current.data.DefaultColumnOptions;
import it.cavallium.dbengine.rpc.current.data.NamedColumnOptions;
import it.cavallium.dbengine.rpc.current.data.nullables.NullableCompression;
import it.cavallium.dbengine.rpc.current.data.nullables.NullableFilter;
import it.cavallium.dbengine.utils.StreamUtils;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class Repair {

	public static final MeterRegistry METER = LoggingMeterRegistry
			.builder(key -> null)
			.clock(Clock.SYSTEM)
			.loggingSink(System.err::println)
			.build();

	public static void main(String[] argsArray) throws RocksDBException {
		ObjectList<String> initialArgs = ObjectArrayList.wrap(argsArray), args = initialArgs;
		if (args.isEmpty() || args.contains("--help")) {
			printHelp(initialArgs);
		}
		String command = args.get(0);
		args = args.subList(1, args.size());
		switch (command.toLowerCase(Locale.ROOT)) {
			case "list-column-families" -> {
				if (args.size() < 3) {
					printHelp(initialArgs);
				}
				Path path = Path.of(args.get(0));
				String dbName = args.get(1);
				getColumnFamilyNames(path, dbName).forEach(s -> System.err.printf("\tColumn family: %s%n", s));
			}
			case "scan-all" -> {
				if (args.size() < 2) {
					printHelp(initialArgs);
				}
				Path path = Path.of(args.get(0)).toAbsolutePath();
				String dbName = args.get(1);
				List<String> columnNames = args.size() < 3 ? getColumnFamilyNames(path, dbName).toList() : args.subList(2, args.size());
				System.err.printf("Scanning database \"%s\" at \"%s\", column families to scan:\n\t%s%n", dbName, path, String.join("\n\t", columnNames));
				var conn = new LLLocalDatabaseConnection(METER, path, false);
				conn.connect();
				LLLocalKeyValueDatabase db = getDatabase(conn, dbName, columnNames);
				db.getAllColumnFamilyHandles().forEach((column, cfh) -> {
					System.err.printf("Scanning column: %s%n", column.name());
					LLLocalDictionary dict = db.getDictionary(column.name().getBytes(StandardCharsets.UTF_8), UpdateMode.DISALLOW);
					StreamUtils.collectOn(StreamUtils.ROCKSDB_POOL, dict.verifyChecksum(LLRange.all()), StreamUtils.executing(block -> {
						synchronized (Repair.class) {
							switch (block) {
								case null -> {}
								case BlockBad blockBad -> {
									System.err.println("[ ! ] Bad block found: " + block.databaseName() + (block.column() != null ? "->" + block.column().name() : "") + "->" + blockBad.rawKey() + "->" + block.file());
									if (blockBad.ex() != null) {
										if (blockBad.ex() instanceof RocksDBException ex) {
											System.err.println("\t" + ex);
										} else {
											blockBad.ex().printStackTrace(System.err);
										}
									}
								}
								case FileOk fileOk -> {
									System.err.println("File is ok: " + block.databaseName() + (block.column() != null ? "->" + block.column().name() : "") + "->" + block.file());
								}
								case Progress progress -> {
									System.err.printf("Progress: [%d/%d] file: [%d/%d] %s->%s->%s%n",
											progress.scanned(),
											progress.total(),
											progress.fileScanned(),
											progress.fileTotal(),
											block.databaseName(),
											block.column() != null ? "->" + block.column().name() : "",
											block.file()
									);
								}
								default -> throw new IllegalStateException("Unexpected value: " + block);
							}
						}
					}));
				});
			}
			case "dump-all" -> {
				if (args.size() < 2) {
					printHelp(initialArgs);
				}
				Path path = Path.of(args.get(0)).toAbsolutePath();
				String dbName = args.get(1);
				List<String> columnNames = args.size() < 3 ? getColumnFamilyNames(path, dbName).toList() : args.subList(2, args.size());
				System.err.printf("Dumping database \"%s\" at \"%s\", column families to dump:\n\t%s%n", dbName, path, String.join("\n\t", columnNames));
				var conn = new LLLocalDatabaseConnection(METER, path, false);
				conn.connect();
				LLLocalKeyValueDatabase db = getDatabase(conn, dbName, columnNames);
				System.out.println("{\"columns\": {");
				AtomicBoolean first = new AtomicBoolean(true);
				db.getAllColumnFamilyHandles().forEach((column, cfh) -> {
					if (!first.compareAndSet(true, false)) {
						System.out.println(",");
					}
					System.out.printf("\"%s\": [", column.name());
					System.err.printf("Dumping column: %s%n", column.name());
					AtomicBoolean firstElem = new AtomicBoolean(true);
					var dict = db.getDictionary(column.name().getBytes(StandardCharsets.UTF_8), UpdateMode.DISALLOW);
					dict.getRange(null, LLRange.all(), false, false).forEach(elem -> {
						if (!firstElem.compareAndSet(true, false)) {
							System.out.println(",");
						} else {
							System.out.println();
						}
						System.out.printf("{\"key\": \"%s\", \"value\": \"%s\"}",
								Base64.getEncoder().encodeToString(elem.getKey().toByteArray()),
								Base64.getEncoder().encodeToString(elem.getValue().toByteArray())
						);
						System.err.printf("\t\tkey: %s\tvalue: %s%n", elem.getKey().toString(), elem.getValue().toString(StandardCharsets.UTF_8));
					});
					System.out.printf("%n]");
				});
				System.out.println();
				System.out.println("}");
				System.out.println("}");
			}
			case "verify-checksum" -> {
				if (args.size() != 2) {
					printHelp(initialArgs);
				}
				Path path = Path.of(args.get(0)).toAbsolutePath();
				String dbName = args.get(1);
				List<String> columnNames = getColumnFamilyNames(path, dbName).toList();
				System.err.printf("Verifying checksum of database \"%s\" at \"%s\", column families:\n\t%s%n", dbName, path, String.join("\n\t", columnNames));
				var conn = new LLLocalDatabaseConnection(METER, path, false);
				conn.connect();
				LLLocalKeyValueDatabase db = getDatabase(conn, dbName, columnNames);
				db.verifyChecksum();
				System.err.println("Done");
			}
			default -> printHelp(initialArgs);
		}
		System.exit(0);
	}

	private static Stream<String> getColumnFamilyNames(Path path, String dbName) throws RocksDBException {
		String dbPath = path.resolve("database_" + dbName).toString();
		System.err.printf("Listing column families of database: %s%n", dbPath);
		try (var options = new Options()) {
			options.setCreateIfMissing(false).setCreateMissingColumnFamilies(false);
			return RocksDB.listColumnFamilies(options, dbPath).stream().map(s -> new String(s, StandardCharsets.UTF_8));
		}
	}

	private static LLLocalKeyValueDatabase getDatabase(LLLocalDatabaseConnection conn,
			String dbName,
			List<String> columnNames) {
		return conn.getDatabase(dbName, columnNames.stream()
				.map(Column::of)
				.toList(), DatabaseOptionsBuilder.builder()
				.openAsSecondary(true)
				.absoluteConsistency(true)
				.allowMemoryMapping(true)
				.blockCache(Nullablelong.empty())
				.lowMemory(false)
				.maxOpenFiles(Nullableint.of(-1))
				.optimistic(false)
				.spinning(false)
				.useDirectIO(false)
				.extraFlags(Map.of())
				.logPath(NullableString.empty())
				.walPath(NullableString.empty())
				.secondaryDirectoryName(NullableString.of("scan-all"))
				.persistentCaches(List.of())
				.volumes(Stream.of(
						DatabaseVolume.of(Path.of(conn.getDatabasePath(dbName).toString()), -1),
						DatabaseVolume.of(Path.of(conn.getDatabasePath(dbName).toString() + "_hot"), -1),
						DatabaseVolume.of(Path.of(conn.getDatabasePath(dbName).toString() + "_cold"), -1),
						DatabaseVolume.of(Path.of(conn.getDatabasePath(dbName).toString() + "_colder"), -1)
				).filter(x -> Files.exists(x.volumePath())).toList())
				.defaultColumnOptions(DefaultColumnOptions.of(
						List.of(),
						Nullablelong.empty(),
						Nullableboolean.empty(),
						Nullableboolean.empty(),
						NullableFilter.empty(),
						Nullableint.empty(),
						NullableString.empty(),
						Nullablelong.empty(),
						false,
						Nullablelong.empty(),
						Nullablelong.empty(),
						NullableCompression.empty()
				))
				.columnOptions(columnNames.stream()
						.map(columnName -> NamedColumnOptions.of(columnName,
								List.of(),
								Nullablelong.empty(),
								Nullableboolean.empty(),
								Nullableboolean.empty(),
								NullableFilter.empty(),
								Nullableint.empty(),
								NullableString.empty(),
								Nullablelong.empty(),
								false,
								Nullablelong.empty(),
								Nullablelong.empty(),
								NullableCompression.empty()))
						.toList())
				.writeBufferManager(Nullablelong.empty())
				.build());
	}

	private static void printHelp(List<String> args) {
		System.err.println("""
		Usage: repair scan-all DIRECTORY DB_NAME COLUMN_NAME...
		   or: repair dump-all DIRECTORY DB_NAME COLUMN_NAME...
		   or: repair verify-checksum DIRECTORY DB_NAME
		   or: repair list-column-families DIRECTORY DB_NAME
		""");
		System.exit(1);
	}
}
