package it.cavallium.dbengine.client;

import it.cavallium.dbengine.database.disk.RocksDBFile.IterationMetadata;
import it.cavallium.dbengine.rpc.current.data.Column;
import org.jetbrains.annotations.Nullable;

public interface SSTProgress {

	record SSTStart(IterationMetadata metadata) implements SSTProgress, SSTVerificationProgress, SSTDumpProgress {}

	record SSTOk(long scannedCount) implements SSTProgress, SSTVerificationProgress, SSTDumpProgress {}

	record SSTProgressReport(long fileScanned, long fileTotal) implements SSTProgress, SSTVerificationProgress,
			SSTDumpProgress {

		public double getFileProgress() {
			if (fileTotal == 0) return 0d;
			return fileScanned / (double) fileTotal;
		}
	}
}
