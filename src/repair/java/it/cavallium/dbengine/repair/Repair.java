package it.cavallium.dbengine.repair;

import static it.cavallium.dbengine.client.DbProgress.toDbProgress;
import static it.cavallium.dbengine.database.disk.LLLocalDatabaseConnection.getDatabasePath;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import it.cavallium.datagen.nativedata.NullableString;
import it.cavallium.datagen.nativedata.Nullableboolean;
import it.cavallium.datagen.nativedata.Nullableint;
import it.cavallium.datagen.nativedata.Nullablelong;
import it.cavallium.dbengine.client.Compression;
import it.cavallium.dbengine.client.DbProgress;
import it.cavallium.dbengine.client.DbProgress.DbSSTProgress;
import it.cavallium.dbengine.client.LongProgressTracker;
import it.cavallium.dbengine.client.SSTDumpProgress;
import it.cavallium.dbengine.client.SSTDumpProgress.SSTBlockKeyValue;
import it.cavallium.dbengine.client.SSTProgress;
import it.cavallium.dbengine.client.SSTProgress.SSTOk;
import it.cavallium.dbengine.client.SSTProgress.SSTProgressReport;
import it.cavallium.dbengine.client.SSTProgress.SSTStart;
import it.cavallium.dbengine.client.SSTVerificationProgress.SSTBlockBad;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.UpdateMode;
import it.cavallium.dbengine.database.disk.RocksDBFile;
import it.cavallium.dbengine.database.disk.SSTRange;
import it.cavallium.dbengine.database.disk.LLLocalDatabaseConnection;
import it.cavallium.dbengine.database.disk.LLLocalDictionary;
import it.cavallium.dbengine.database.disk.LLLocalKeyValueDatabase;
import it.cavallium.dbengine.database.disk.SSTRange.SSTRangeFull;
import it.cavallium.dbengine.database.disk.rocksdb.LLSstFileWriter;
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
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributeView;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileWriter;

public class Repair {

	public static final MeterRegistry METER = new SimpleMeterRegistry(); // LoggingMeterRegistry.builder(key -> null).clock(Clock.SYSTEM).loggingSink(System.err::println).build();

	static final boolean PRINT_ALL_CHECKSUM_VERIFICATION_STEPS
			= Boolean.parseBoolean(System.getProperty("it.cavallium.dbengine.checks.verification.print", "false"));

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
				try (var os = getErrorLogFile(dbName)) {
					db.getAllColumnFamilyHandles().forEach((column, cfh) -> {
						System.err.printf("Scanning column: %s%n", column.name());
						LLLocalDictionary dict = db.getDictionary(column.name().getBytes(StandardCharsets.UTF_8), UpdateMode.DISALLOW);
						LongProgressTracker fileCountTotalTracker = new LongProgressTracker(0);
						StreamUtils.collectOn(ForkJoinPool.commonPool(),
								peekProgress(os, fileCountTotalTracker, 0, dict.verifyChecksum(LLRange.all())),
								StreamUtils.executing()
						);
					});
				}
			}
			case "scan-files" -> {
				if (args.size() < 3) {
					printHelp(initialArgs);
				}
				Path path = Path.of(args.get(0)).toAbsolutePath();
				String dbName = args.get(1);
				boolean skip = args.get(2).equalsIgnoreCase("--skip");
				List<String> fileNames = args.subList(skip ? 3 : 2, args.size()).stream().map(f -> f.startsWith("/") ? f : ("/" + f)).toList();
				List<String> columnNames = getColumnFamilyNames(path, dbName).toList();
				System.err.printf("Scanning database \"%s\" at \"%s\", files:%n%s%n", dbName, path, String.join("\n", fileNames));
				var conn = new LLLocalDatabaseConnection(METER, path, false);
				conn.connect();
				LLLocalKeyValueDatabase db = getDatabase(conn, dbName, columnNames);
				try (var os = getErrorLogFile(dbName)) {
					AtomicLong ignoredFiles = new AtomicLong();
					var fileList = db.getAllLiveFiles()
							.filter(file -> {
								if (file.getSstNumber() == null) {
									System.err.printf("Empty SST number: %s%n", file);
								}
								if (!skip && !fileNames.contains(file.getMetadata().fileName())) {
									ignoredFiles.incrementAndGet();
									System.err.printf("Ignoring file: \"%s\"%n", file.getMetadata().fileName());
									return false;
								} else if (skip && fileNames.contains(file.getMetadata().fileName())) {
									ignoredFiles.incrementAndGet();
									System.err.printf("Ignoring file: \"%s\"%n", file.getMetadata().fileName());
									return false;
								} else {
									System.err.printf("About to scan file: \"%s\"%n", file.getMetadata().fileName());
									return true;
								}
							})
							.sorted(Comparator.reverseOrder())
							.toList();
					var keyTotalTracker = new LongProgressTracker();
					var parallelism = Math.max(1, Runtime.getRuntime().availableProcessors());
					try (var pool = Executors.newWorkStealingPool(parallelism)) {
						LongProgressTracker fileCountTotalTracker = new LongProgressTracker(fileList.size());
						for (int i = 0; i < parallelism; i++) {
							var iF = i;

							pool.execute(() -> peekProgress(os, fileCountTotalTracker, ignoredFiles.get(),

									IntStream.range(0, fileList.size())
									.filter(n -> n % parallelism == iF)
									.mapToObj(fileList::get)

									.flatMap(file -> toDbProgress(dbName, "any", keyTotalTracker, file.verify(new SSTRangeFull()))))
									.sequential()
									.forEachOrdered(x -> {}));
						}
					}
				}
			}
			case "export-files" -> {
				if (args.size() < 4) {
					printHelp(initialArgs);
				}
				Path path = Path.of(args.get(0)).toAbsolutePath();
				String dbName = args.get(1);
				Path dbPath = getDatabasePath(path, dbName);
				Path destPath = Path.of(args.get(2)).toAbsolutePath();
				record Input(String fileName, SSTRange exportOption) {

					@Override
					public String toString() {
						return Input.this.fileName + "[" + Input.this.exportOption + "]";
					}
				}
				Map<String, Input> fileNames = args.subList(3, args.size()).stream().<Input>map(f -> {
					var fn = f.startsWith("/") ? f : ("/" + f);
					if (fn.indexOf('[') == -1) {
						fn = fn + "[full]";
					}
					var fileName = StringUtils.substringBeforeLast(fn, "[");
					var exportOptions = SSTRange.parse(StringUtils.substringBetween(fn, "[", "]"));
					return new Input(fileName, exportOptions);
				}).collect(Collectors.toMap(Input::fileName, Function.identity()));
				List<String> columnNames = getColumnFamilyNames(path, dbName).toList();
				System.err.printf("Exporting database \"%s\" at \"%s\" to \"%s\", files:%n%s%n", dbName, path, destPath, String.join("\n", fileNames.values().stream().map(Input::toString).toList()));
				var conn = new LLLocalDatabaseConnection(METER, path, false);
				conn.connect();
				try (var os = getErrorLogFile(dbName)) {
					var fileList = fileNames.values().stream()
							.map(input -> Map.entry(new RocksDBFile(dbPath, input.fileName()), input.exportOption()))
							.toList();
					var keyTotalTracker = new LongProgressTracker();
					LongProgressTracker fileCountTotalTracker = new LongProgressTracker(fileList.size());
					peekProgress(os, fileCountTotalTracker, 0, fileList.stream().flatMap(e ->
							toDbProgress(dbName, "any", keyTotalTracker,
									peekExportSST(destPath, e.getKey().readAllSST(e.getValue(), false))))
					).sequential().forEachOrdered(x -> {});
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
					System.out.printf("%s\t%s\t%s\t%s\t%s\t%d\t%d%n", meta.fileName(), meta.filePath(), meta.columnName(), meta.keysRange(), new DataSize(meta.size()).toString(false), meta.numEntries(), meta.level());
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

	private static Stream<SSTDumpProgress> peekExportSST(Path destDirectory, Stream<SSTDumpProgress> sstProgressStream) {
		try {
			Files.createDirectories(destDirectory);
		} catch (IOException e) {
			throw new CompletionException(e);
		}
		return StreamUtils.resourceStream(() -> new LLSstFileWriter(false), w -> {
			AtomicInteger fileNum = new AtomicInteger(1);
			AtomicReference<String> fileName = new AtomicReference<>();
			AtomicLong memory = new AtomicLong(0);
			SstFileWriter writer = w.get();
			return sstProgressStream.peek(r -> {
				synchronized (writer) {
					try {
						switch (r) {
							case SSTStart start -> {
								var sst = destDirectory.resolve(start.metadata().filePath().getFileName()).toString();
								fileName.set(sst);
								System.err.printf("Creating SST at \"%s\"%n", sst);
								writer.open(sst);
							}
							case SSTBlockKeyValue report -> {
								var k = report.rawKey().toByteArray();
								var v = report.rawValue().toByteArray();
								long currentMem = k.length + v.length;
								writer.put(k, v);
								var memVal = memory.addAndGet(currentMem);
								// 4GiB
								if (memVal > 4L * 1024 * 1024 * 1024) {
									memory.set(0);
									writer.finish();
									writer.open(fileName + "." + fileNum.getAndIncrement() + ".sst");
								}
							}
							case SSTOk ok -> writer.finish();
							default -> {
							}
						}
					} catch (RocksDBException ex) {
						throw new CompletionException(ex);
					}
				}
			});
		});
	}

	private static BufferedWriter getErrorLogFile(String dbName) throws IOException {
		Path cwd = Path.of(".");
		String errorsFileNamePrefix = "errors-" + dbName;
		String errorsFileNameSuffix = ".log";
		Path errorsFile;
		if (Files.isWritable(cwd)) {
			errorsFile = cwd.resolve(errorsFileNamePrefix + "-" + System.currentTimeMillis() + errorsFileNameSuffix);
		} else {
			errorsFile = Files.createTempFile(errorsFileNamePrefix, errorsFileNameSuffix);
		}
		return Files.newBufferedWriter(errorsFile,
				StandardCharsets.UTF_8,
				StandardOpenOption.CREATE,
				StandardOpenOption.TRUNCATE_EXISTING,
				StandardOpenOption.DSYNC
		);
	}

	private static <T extends DbProgress<? extends SSTProgress>> Stream<T> peekProgress(@Nullable BufferedWriter os, LongProgressTracker fileCountTotalTracker,
			long ignoredFiles, Stream<T> verificationProgressStream) {
		var showProgress = !Boolean.getBoolean("it.cavallium.dbengine.repair.hideprogress");
		AtomicLong startMs = new AtomicLong();
		return verificationProgressStream.peek(block -> {
			synchronized (Repair.class) {
				switch (block) {
					case null -> {}
					case DbSSTProgress<? extends SSTProgress> dbSstProgress -> {
						var sstProgress = dbSstProgress.sstProgress();
						switch (sstProgress) {
							case SSTStart sstStart -> {
								startMs.set(System.currentTimeMillis());
								System.err.printf("Processing file [%d/%d%s]: %s%n", fileCountTotalTracker.incrementAndGet(),
										fileCountTotalTracker.getTotal(),
										(ignoredFiles > 0 ? " (+%d ignored)".formatted(ignoredFiles) : ""), sstStart.metadata().filename());
								String date;
								try {
									var dateInstant = Files.getFileAttributeView(sstStart.metadata().filePath(), BasicFileAttributeView.class).readAttributes().creationTime().toInstant().atZone(ZoneId.systemDefault());
									date = DateTimeFormatter.ofLocalizedDateTime(FormatStyle.FULL).format(dateInstant);
								} catch (Exception e) {
									date = "unknown";
									e.printStackTrace();
								}
								System.err.println(
										"File scan begin: " + block.databaseName() + (dbSstProgress.column() != null ? "->" + dbSstProgress.column().name() : "") + "->" + dbSstProgress.fileString() + ". The file creation date is: " + date);
								if (PRINT_ALL_CHECKSUM_VERIFICATION_STEPS) {
									System.err.printf("Seeking to %s->%s->first on file %s%n", block.databaseName(), dbSstProgress.column(), dbSstProgress.file());
								}
							}
							case SSTBlockBad blockBad -> {
								StringBuilder errorLineBuilder = new StringBuilder()
										.append("File scan ended with error, bad block found: ")
										.append(block.databaseName())
										.append(dbSstProgress.column() != null ? "->" + dbSstProgress.column().name() : "")
										.append("->")
										.append(blockBad.rawKey())
										.append("->")
										.append(dbSstProgress.file());
								if (blockBad.ex() != null) {
									if (blockBad.ex() instanceof RocksDBException ex) {
										errorLineBuilder.append("\n\t").append(ex);
									} else {
										errorLineBuilder.append("\n").append(ExceptionUtils.getStackTrace(blockBad.ex()));
									}
								}
								String errorLine = errorLineBuilder.toString();
								System.err.println("[ ! ] " + errorLine);
								if (os != null) {
									try {
										os.write(errorLine);
										os.flush();
									} catch (IOException e) {
										System.err.println("Can't write to errors file: " + e);
									}
								}
							}
							case SSTOk end -> {
								var scanned = end.scannedCount();
								var time = Duration.ofMillis(System.currentTimeMillis() - startMs.get());
								var rate = (int) (scanned / Math.max(1, time.toMillis() / 1000d));
								System.err.println("File scan ended with success: " + block.databaseName() + (dbSstProgress.column() != null ? "->" + dbSstProgress.column().name() : "") + "->" + dbSstProgress.fileString() + " - " + end.scannedCount() + " keys scanned in " + time + ". Speed: " + rate + "keys/sec.");
							}
							case SSTProgressReport progress -> {
								if (showProgress) {
									boolean shouldSendStatus = progress.fileScanned() % 1_000_000 == 0;
									if (PRINT_ALL_CHECKSUM_VERIFICATION_STEPS || shouldSendStatus) {
										System.err.printf("Progress: %s[%d/%d] %s%s->%s%n",
												dbSstProgress.total() != -1 ? "[%d/%d] file: ".formatted(dbSstProgress.scanned(), dbSstProgress.total()) : "",
												progress.fileScanned(),
												progress.fileTotal(),
												block.databaseName(),
												dbSstProgress.column() != null ? "->" + dbSstProgress.column().name() : "",
												dbSstProgress.fileString()
										);
									}
								}
							}
							default -> {}
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
				.blockCache(Nullablelong.of(4L * 1024 * 1024 * 1024))
				.lowMemory(false)
				.maxOpenFiles(Nullableint.of(Math.max(40, Runtime.getRuntime().availableProcessors() * 3)))
				.optimistic(false)
				.spinning(true)
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
						Nullablelong.of(512 * 1024 * 1024),
						Nullableboolean.of(true),
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
								Nullablelong.of(512 * 1024 * 1024),
								Nullableboolean.of(true),
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
		   or: repair scan-files DIRECTORY DB_NAME [--skip] FILE-NAME...
		   or: repair export-files DIRECTORY DB_NAME DESTINATION_DIRECTORY '{fileName}[{key|offset}-{from}-{to}]'...
		   or: repair list-files DIRECTORY DB_NAME
		""");
		System.exit(1);
	}
}
