package it.cavallium.dbengine.client;

import io.netty.buffer.Unpooled;
import it.cavallium.dbengine.database.Column;
import it.cavallium.dbengine.database.collections.DatabaseMapDictionary;
import it.cavallium.dbengine.database.collections.FixedLengthSerializer;
import it.cavallium.dbengine.database.collections.SubStageGetterSingle;
import it.cavallium.dbengine.database.disk.LLLocalDatabaseConnection;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.One;

public class Example {

	public static void main(String[] args) {
		System.out.println("Test");
		var ssg = new SubStageGetterSingle();
		var ser = FixedLengthSerializer.noop(4);
		var itemKey = new byte[] {0, 1, 2, 3};
		var newValue = new byte[] {4, 5, 6, 7};
		var itemKeyBuffer = Unpooled.wrappedBuffer(itemKey);
		One<Instant> instantFirst = Sinks.one();
		One<Instant> instantSecond = Sinks.one();
		One<Instant> instantThird = Sinks.one();
		new LLLocalDatabaseConnection(Path.of("/tmp/"), true)
				.connect()
				.flatMap(conn -> conn.getDatabase("testdb", List.of(Column.dictionary("testmap")), false))
				.flatMap(db -> db.getDictionary("testmap"))
				.map(dictionary -> new DatabaseMapDictionary<>(dictionary, ssg, ser, 10))
				.doOnSuccess(s -> System.out.println("Setting new value at key " + Arrays.toString(itemKey) + ": " + Arrays.toString(newValue)))
				.doOnSuccess(s -> instantFirst.tryEmitValue(Instant.now()))
				.flatMap(map -> map.at(null, itemKeyBuffer))
				.doOnSuccess(s -> instantSecond.tryEmitValue(Instant.now()))
				.flatMap(handle -> handle.setAndGetPrevious(newValue))
				.doOnSuccess(s -> instantThird.tryEmitValue(Instant.now()))
				.doOnSuccess(oldValue -> System.out.println("Old value: " + (oldValue == null ? "None" : Arrays.toString(oldValue))))
				.then(Mono.zip(instantFirst.asMono(), instantSecond.asMono(), instantThird.asMono()))
				.doOnSuccess(s -> {
					System.out.println("Time to get value reference: " + Duration.between(s.getT1(), s.getT2()).toMillis() + "ms");
					System.out.println("Time to set new value and get previous: " + Duration.between(s.getT2(), s.getT3()).toMillis() + "ms");
					System.out.println("(Total time) " + Duration.between(s.getT1(), s.getT3()).toMillis() + "ms");
				})
				.blockOptional();
	}
}