package it.cavallium.dbengine.client;

import com.google.common.primitives.Ints;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import it.cavallium.dbengine.database.Column;
import it.cavallium.dbengine.database.LLKeyValueDatabase;
import it.cavallium.dbengine.database.collections.DatabaseMapDictionaryDeep;
import it.cavallium.dbengine.database.collections.SubStageGetterSingle;
import it.cavallium.dbengine.database.collections.SubStageGetterSingleBytes;
import it.cavallium.dbengine.database.disk.LLLocalDatabaseConnection;
import it.cavallium.dbengine.database.serialization.Codec;
import it.cavallium.dbengine.database.serialization.CodecSerializer;
import it.cavallium.dbengine.database.serialization.Codecs;
import it.cavallium.dbengine.database.serialization.SerializerFixedBinaryLength;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.CompletionException;
import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuples;

public class CodecsExample {

	public static void main(String[] args) {
		testConversionSpeed();
	}

	private static void testConversionSpeed() {
		SpeedExample.test("No-Op Conversion",
				tempDb(true)
						.flatMap(db -> db.getDictionary("testmap").map(dict -> Tuples.of(db, dict)))
						.map(tuple -> tuple.mapT2(dict -> Tuples.of(DatabaseMapDictionaryDeep.simple(dict,
								SerializerFixedBinaryLength.longSerializer(),
								new SubStageGetterSingleBytes()
								),
								DatabaseMapDictionaryDeep.simple(dict,
										SerializerFixedBinaryLength.longSerializer(),
										new SubStageGetterSingleBytes()
								)
						))),
				tuple -> Flux
						.range(0, SpeedExample.batchSize)
						.flatMap(n -> tuple
								.getT2()
								.getT1()
								.putValue(n * 7L, Ints.toByteArray(n * 3))
								.then(tuple.getT2().getT2().getValue(null, n * 7L)))
						.then(),
				SpeedExample.numRepeats,
				tuple -> tuple.getT1().close()
		).then(
				SpeedExample.test("Conversion",
						tempDb(true)
								.flatMap(db -> db.getDictionary("testmap").map(dict -> Tuples.of(db, dict)))
								.map(tuple -> tuple.mapT2(dict -> Tuples.of(DatabaseMapDictionaryDeep.simple(dict,
										SerializerFixedBinaryLength.longSerializer(),
										getOldSubStageGetter()
										),
										DatabaseMapDictionaryDeep.simple(dict,
												SerializerFixedBinaryLength.longSerializer(),
												getNewSubStageGetter()
										)
								))),
						tuple -> Flux
								.range(0, SpeedExample.batchSize)
								.flatMap(n -> tuple
										.getT2()
										.getT1()
										.putValue(n * 7L, new OldCustomType(n * 3))
										.then(tuple.getT2().getT2().getValue(null, n * 7L)))
								.then(),
						SpeedExample.numRepeats,
						tuple -> tuple.getT1().close()
				)).subscribeOn(Schedulers.parallel()).blockOptional();
	}

	private static void testConversion() {
		writeOld()
				.then()
				.then(readNew())
				.subscribeOn(Schedulers.parallel())
				.blockOptional();
	}

	private static SubStageGetterSingle<CurrentCustomType> getNewSubStageGetter() {
		var newCodec = new NewCustomTypeCodecV2();
		var newCodecs = new Codecs<CurrentCustomType>();
		newCodecs.registerCodec(1, new NewCustomTypeCodecV1());
		newCodecs.registerCodec(2, newCodec);
		var newSerializer = new CodecSerializer<>(newCodecs, newCodec, 2, true);
		return new SubStageGetterSingle<>(newSerializer);
	}

	private static SubStageGetterSingle<OldCustomType> getOldSubStageGetter() {
		var oldCodec = new OldCustomTypeCodec();
		var oldCodecs = new Codecs<OldCustomType>();
		oldCodecs.registerCodec(1, oldCodec);
		var oldSerializer = new CodecSerializer<>(oldCodecs, oldCodec, 1, true);
		return new SubStageGetterSingle<>(oldSerializer);
	}

	private static Mono<Void> readNew() {
		return tempDb(false)
				.flatMap(db -> db.getDictionary("testmap").map(dict -> Tuples.of(db, dict)))
				.map(tuple -> tuple.mapT2(dict -> DatabaseMapDictionaryDeep.simple(dict,
						SerializerFixedBinaryLength.longSerializer(),
						getNewSubStageGetter()
				)))
				.flatMap(tuple -> {
					System.out.println("Reading from disk current value with any codec id...");
					return tuple.getT2().getValue(null, 15L).doOnSuccess(s -> {
						if (s == null) {
							System.err.println("No value found for key 15");
						} else {
							System.out.println("Current value read successfully: " + s);
						}
					}).then(tuple.getT1().close());
				});
	}

	private static Mono<Void> writeOld() {

		return tempDb(true)
				.flatMap(db -> db.getDictionary("testmap").map(dict -> Tuples.of(db, dict)))
				.map(tuple -> tuple.mapT2(dict -> DatabaseMapDictionaryDeep.simple(dict,
						SerializerFixedBinaryLength.longSerializer(),
						getOldSubStageGetter()
				)))
				.flatMap(tuple -> {
					var oldValue = new OldCustomType(155);
					System.out.println("Writing to disk old value with codec id 1: " + oldValue);

					return tuple.getT2().putValue(15L, oldValue).then(tuple.getT1().close());
				});
	}

	private static class OldCustomTypeCodec implements Codec<OldCustomType> {

		@Override
		public @NotNull OldCustomType deserialize(@NotNull ByteBufInputStream serialized) throws IOException {
			return new OldCustomType(serialized.readUTF());
		}

		@Override
		public void serialize(@NotNull ByteBufOutputStream outputStream, @NotNull OldCustomType deserialized) throws IOException {
			outputStream.writeUTF(deserialized.number);
		}
	}

	private static class NewCustomTypeCodecV1 implements Codec<CurrentCustomType> {

		@Override
		public @NotNull CurrentCustomType deserialize(@NotNull ByteBufInputStream serialized) throws IOException {
			return new CurrentCustomType(Integer.parseInt(serialized.readUTF()));
		}

		@Override
		public void serialize(@NotNull ByteBufOutputStream outputStream, @NotNull CurrentCustomType deserialized) throws IOException {
			throw new UnsupportedOperationException("Can't serialize with an old version");
		}
	}

	private static class NewCustomTypeCodecV2 implements Codec<CurrentCustomType> {

		@Override
		public @NotNull CurrentCustomType deserialize(@NotNull ByteBufInputStream serialized) throws IOException {
			return new CurrentCustomType(serialized.readInt());
		}

		@Override
		public void serialize(@NotNull ByteBufOutputStream outputStream, @NotNull CurrentCustomType deserialized) throws IOException {
			outputStream.writeInt(deserialized.number);
		}
	}

	public static final class OldCustomType {

		private final String number;

		public OldCustomType(int number) {
			this.number = "" + number;
		}

		public OldCustomType(String readUTF) {
			this.number = readUTF;
		}

		public String getNumber() {
			return number;
		}

		@Override
		public String toString() {
			return new StringJoiner(", ", OldCustomType.class.getSimpleName() + "[", "]")
					.add("number='" + number + "'")
					.toString();
		}
	}

	public static final class CurrentCustomType {

		private final int number;

		public CurrentCustomType(int number) {
			this.number = number;
		}

		public int getNumber() {
			return number;
		}

		@Override
		public String toString() {
			return new StringJoiner(", ", CurrentCustomType.class.getSimpleName() + "[", "]")
					.add("number=" + number)
					.toString();
		}
	}

	private static <U> Mono<? extends LLKeyValueDatabase> tempDb(boolean delete) {
		var wrkspcPath = Path.of("/tmp/tempdb/");
		return Mono
				.fromCallable(() -> {
					if (delete && Files.exists(wrkspcPath)) {
						Files.walk(wrkspcPath)
								.sorted(Comparator.reverseOrder())
								.forEach(file -> {
									try {
										Files.delete(file);
									} catch (IOException ex) {
										throw new CompletionException(ex);
									}
								});
					}
					Files.createDirectories(wrkspcPath);
					return null;
				})
				.subscribeOn(Schedulers.boundedElastic())
				.then(new LLLocalDatabaseConnection(wrkspcPath, true).connect())
				.flatMap(conn -> conn.getDatabase("testdb", List.of(Column.dictionary("testmap")), false));
	}
}
