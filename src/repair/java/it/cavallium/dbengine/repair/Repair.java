package it.cavallium.dbengine.repair;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import it.cavallium.datagen.nativedata.NullableString;
import it.cavallium.datagen.nativedata.Nullableboolean;
import it.cavallium.datagen.nativedata.Nullableint;
import it.cavallium.datagen.nativedata.Nullablelong;
import it.cavallium.dbengine.client.Compression;
import it.cavallium.dbengine.client.VerificationProgress;
import it.cavallium.dbengine.client.VerificationProgress.BlockBad;
import it.cavallium.dbengine.client.VerificationProgress.FileOk;
import it.cavallium.dbengine.client.VerificationProgress.FileStart;
import it.cavallium.dbengine.client.VerificationProgress.Progress;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.UpdateMode;
import it.cavallium.dbengine.database.disk.LLLocalDatabaseConnection;
import it.cavallium.dbengine.database.disk.LLLocalDictionary;
import it.cavallium.dbengine.database.disk.LLLocalKeyValueDatabase;
import it.cavallium.dbengine.database.disk.RocksDBFile;
import it.cavallium.dbengine.rpc.current.data.Column;
import it.cavallium.dbengine.rpc.current.data.DatabaseOptionsBuilder;
import it.cavallium.dbengine.rpc.current.data.DatabaseVolume;
import it.cavallium.dbengine.rpc.current.data.DefaultColumnOptions;
import it.cavallium.dbengine.rpc.current.data.NamedColumnOptions;
import it.cavallium.dbengine.rpc.current.data.nullables.NullableCompression;
import it.cavallium.dbengine.rpc.current.data.nullables.NullableFilter;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class Repair {

	public static final MeterRegistry METER = new SimpleMeterRegistry(); // LoggingMeterRegistry.builder(key -> null).clock(Clock.SYSTEM).loggingSink(System.err::println).build();

	public static void main(String[] argsArray) throws RocksDBException, IOException {
		System.setProperty("it.cavallium.dbengine.checks.compression", "true");
		System.setProperty("it.cavallium.dbengine.checks.paranoid", "true");
		System.setProperty("it.cavallium.dbengine.checks.filesize", "true");
		System.setProperty("it.cavallium.dbengine.checks.paranoidfilechecks", "true");
		System.setProperty("it.cavallium.dbengine.checks.forcecolumnfamilyconsistencychecks", "true");
		System.setProperty("it.cavallium.dbengine.log.levelcode", String.valueOf(InfoLogLevel.DEBUG_LEVEL.getValue()));
		ObjectList<String> initialArgs = ObjectArrayList.wrap(argsArray), args = initialArgs;
		if (args.isEmpty() || args.contains("--help")) {
			printHelp(initialArgs);
		}
		String command = args.get(0);
		args = args.subList(1, args.size());
		switch (command.toLowerCase(Locale.ROOT)) {
			case "list-column-families" -> {
				if (args.size() != 2) {
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
				Path cwd = Path.of(".");
				String errorsFileNamePrefix = "errors-" + dbName;
				String errorsFileNameSuffix = ".log";
				Path errorsFile;
				if (Files.isWritable(cwd)) {
					errorsFile = cwd.resolve(errorsFileNamePrefix + "-" + System.currentTimeMillis() + errorsFileNameSuffix);
				} else {
					errorsFile = Files.createTempFile(errorsFileNamePrefix, errorsFileNameSuffix);
				}
				try (var os = Files.newBufferedWriter(errorsFile,
						StandardCharsets.UTF_8,
						StandardOpenOption.CREATE,
						StandardOpenOption.TRUNCATE_EXISTING,
						StandardOpenOption.DSYNC
				)) {
					db.getAllColumnFamilyHandles().forEach((column, cfh) -> {
						System.err.printf("Scanning column: %s%n", column.name());
						LLLocalDictionary dict = db.getDictionary(column.name().getBytes(StandardCharsets.UTF_8), UpdateMode.DISALLOW);
						consumeVerification(os, dict.verifyChecksum(LLRange.all()));
					});
				}
			}
			case "scan-files" -> {
				if (args.size() < 3) {
					printHelp(initialArgs);
				}
				Path path = Path.of(args.get(0)).toAbsolutePath();
				String dbName = args.get(1);
				List<String> fileNames = args.subList(2, args.size());
				List<String> columnNames = getColumnFamilyNames(path, dbName).toList();
				System.err.printf("Scanning database \"%s\" at \"%s\", files:%n%s%n", dbName, path, String.join("\n", fileNames));
				var conn = new LLLocalDatabaseConnection(METER, path, false);
				conn.connect();
				LLLocalKeyValueDatabase db = getDatabase(conn, dbName, columnNames);
				Path cwd = Path.of(".");
				String errorsFileNamePrefix = "errors-" + dbName;
				String errorsFileNameSuffix = ".log";
				Path errorsFile;
				if (Files.isWritable(cwd)) {
					errorsFile = cwd.resolve(errorsFileNamePrefix + "-" + System.currentTimeMillis() + errorsFileNameSuffix);
				} else {
					errorsFile = Files.createTempFile(errorsFileNamePrefix, errorsFileNameSuffix);
				}
				try (var os = Files.newBufferedWriter(errorsFile,
						StandardCharsets.UTF_8,
						StandardOpenOption.CREATE,
						StandardOpenOption.TRUNCATE_EXISTING,
						StandardOpenOption.DSYNC
				)) {
					consumeVerification(os, db.getAllLiveFiles()
							.filter(file -> {
								if (fileNames.contains(file.getMetadata().fileName())) {
									return true;
								} else {
									System.err.printf("Ignoring file: \"%s\"%n", file.getMetadata().fileName());
									return false;
								}
							})
							.flatMap(file -> file.verify(dbName, "any", LLRange.all())));
				}
			}
			case "list-files" -> {
				if (args.size() != 2) {
					printHelp(initialArgs);
				}
				Path path = Path.of(args.get(0)).toAbsolutePath();
				String dbName = args.get(1);
				List<String> columnNames = getColumnFamilyNames(path, dbName).toList();
				System.err.printf("Getting files of database \"%s\" at \"%s\"%n", dbName, path);
				System.out.println("Name \tPath \tColumn name \tKeys range \tSize \tEntries \tLevel");
				var conn = new LLLocalDatabaseConnection(METER, path, false);
				conn.connect();
				LLLocalKeyValueDatabase db = getDatabase(conn, dbName, columnNames);
				db.getAllLiveFiles().sorted(Comparator
						.<RocksDBFile>comparingInt(x -> x.getMetadata().level())
						.thenComparingLong(x -> x.getMetadata().size())
						.thenComparingLong(x -> x.getMetadata().numEntries())
						.thenComparing(x -> x.getMetadata().fileName())
				).forEach(file -> {
					var meta = file.getMetadata();
					System.out.printf("%s\t%s\t%s\t%s\t%s\t%d\t%d%n", meta.fileName(), meta.path(), meta.columnName(), meta.keysRange(), new DataSize(meta.size()).toString(false), meta.numEntries(), meta.level());
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

	private static void consumeVerification(BufferedWriter os, Stream<VerificationProgress> verificationProgressStream) {
		var showProgress = !Boolean.getBoolean("it.cavallium.dbengine.repair.hideprogress");
		verificationProgressStream.parallel().forEach(block -> {
			synchronized (Repair.class) {
				switch (block) {
					case null -> {}
					case BlockBad blockBad -> {
						StringBuilder errorLineBuilder = new StringBuilder()
								.append("File scan ended with error, bad block found: ")
								.append(block.databaseName())
								.append(block.column() != null ? "->" + block.column().name() : "")
								.append("->")
								.append(blockBad.rawKey())
								.append("->")
								.append(block.file());
						if (blockBad.ex() != null) {
							if (blockBad.ex() instanceof RocksDBException ex) {
								errorLineBuilder.append("\n\t").append(ex);
							} else {
								errorLineBuilder.append("\n").append(ExceptionUtils.getStackTrace(blockBad.ex()));
							}
						}
						String errorLine = errorLineBuilder.toString();
						System.err.println("[ ! ] " + errorLine);
						try {
							os.write(errorLine);
							os.flush();
						} catch (IOException e) {
							System.err.println("Can't write to errors file: " + e);
						}
					}
					case FileOk ignored ->
							System.err.println("File scan ended with success: " + block.databaseName() + (block.column() != null ? "->" + block.column().name() : "") + "->" + block.file());
					case FileStart ignored ->
							System.err.println("File scan begin: " + block.databaseName() + (block.column() != null ? "->" + block.column().name() : "") + "->" + block.file());
					case Progress progress -> {
						if (showProgress) {
							System.err.printf("Progress: %s[%d/%d] %s%s->%s%n",
									progress.total() != -1 ? "[%d/%d] file: ".formatted(progress.scanned(), progress.total()) : "",
									progress.fileScanned(),
									progress.fileTotal(),
									block.databaseName(),
									block.column() != null ? "->" + block.column().name() : "",
									block.file()
							);
						}
					}
					default -> throw new IllegalStateException("Unexpected value: " + block);
				}
			}
		});
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
						true,
						Nullablelong.of(8096 * 1024),
						Nullablelong.empty(),
						NullableCompression.of(Compression.LZ4_HC)
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
								true,
								Nullablelong.of(8096 * 1024),
								Nullablelong.empty(),
								NullableCompression.of(Compression.LZ4_HC)))
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
		   or: repair scan-files DIRECTORY DB_NAME FILE-NAME...
		   or: repair list-files DIRECTORY DB_NAME
		""");
		System.exit(1);
	}
}
