package it.cavallium.dbengine.client;

import it.cavallium.dbengine.client.SSTProgress.SSTProgressReport;
import it.cavallium.dbengine.client.SSTProgress.SSTStart;
import it.cavallium.dbengine.rpc.current.data.Column;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.jetbrains.annotations.Nullable;

public interface DbProgress<T extends SSTProgress> {

	String databaseName();

	record DbSSTProgress<T extends SSTProgress>(String databaseName, Column column, @Nullable Path file, long scanned,
																							long total, T sstProgress) implements DbProgress<T> {

		public double getProgress() {
			if (total == 0) {
				return 0d;
			}
			return scanned / (double) total;
		}

		public String fileString() {
			return file != null ? file.normalize().toString() : null;
		}
	}

	static <T extends SSTProgress> Stream<DbProgress<T>> toDbProgress(String dbName,
			String columnName,
			LongProgressTracker totalTracker,
			Stream<T> stream) {
		Column column = Column.of(columnName);
		AtomicReference<Path> filePath = new AtomicReference<>();
		return stream.map(state -> {
			switch (state) {
				case SSTStart start -> filePath.set(start.metadata().filePath());
				case SSTProgressReport progress -> totalTracker.incrementAndGet();
				default -> {}
			}
			return new DbSSTProgress<>(dbName, column, filePath.get(), totalTracker.getCurrent(), totalTracker.getTotal(), state);
		});
	}
}
