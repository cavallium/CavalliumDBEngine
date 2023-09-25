package it.cavallium.dbengine.client;

import it.cavallium.buffer.Buf;
import it.cavallium.dbengine.rpc.current.data.Column;
import org.jetbrains.annotations.Nullable;

public sealed interface VerificationProgress {
	record BlockBad(String databaseName, Column column, Buf rawKey, String file, Throwable ex)
			implements VerificationProgress {}
	record FileOk(String databaseName, Column column, String file)
			implements VerificationProgress {}
	record Progress(String databaseName, Column column, String file,
									long scanned, long total,
									long fileScanned, long fileTotal)
			implements VerificationProgress {

		public double getProgress() {
			return scanned / (double) total;
		}

		public double getFileProgress() {
			return fileScanned / (double) fileTotal;
		}
	}

	@Nullable String databaseName();
	@Nullable Column column();
	@Nullable String file();
}
