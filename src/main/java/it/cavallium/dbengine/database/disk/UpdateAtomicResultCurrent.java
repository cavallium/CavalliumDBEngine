package it.cavallium.dbengine.database.disk;

import io.netty5.buffer.Buffer;
import io.netty5.util.Send;

public record UpdateAtomicResultCurrent(Buffer current) implements UpdateAtomicResult {

	@Override
	public void close() {
		if (current != null && current.isAccessible()) {
			current.close();
		}
	}
}
