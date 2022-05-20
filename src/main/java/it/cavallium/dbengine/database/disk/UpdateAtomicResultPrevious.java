package it.cavallium.dbengine.database.disk;

import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.Send;

public record UpdateAtomicResultPrevious(Buffer previous) implements UpdateAtomicResult {

	@Override
	public void close() {
		if (previous != null && previous.isAccessible()) {
			previous.close();
		}
	}
}
