package it.cavallium.dbengine.database.disk;

import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.Send;

public record UpdateAtomicResultCurrent(Buffer current) implements UpdateAtomicResult {

	@Override
	public void close() {
		if (current != null && current.isAccessible()) {
			current.close();
		}
	}
}
