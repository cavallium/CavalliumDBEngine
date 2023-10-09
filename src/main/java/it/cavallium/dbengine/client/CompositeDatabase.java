package it.cavallium.dbengine.client;

import io.micrometer.core.instrument.MeterRegistry;
import it.cavallium.dbengine.database.DatabaseOperations;
import it.cavallium.dbengine.database.DatabaseProperties;
import java.util.stream.Stream;

public interface CompositeDatabase extends DatabaseProperties, DatabaseOperations {

	void preClose();

	void close();

	/**
	 * Can return SnapshotException
	 */
	CompositeSnapshot takeSnapshot();

	/**
	 * Can return SnapshotException
	 */
	void releaseSnapshot(CompositeSnapshot snapshot);

	MeterRegistry getMeterRegistry();

	/**
	 * Find corrupted items
	 */
	Stream<DbProgress<SSTVerificationProgress>> verify();

	void verifyChecksum();
}
