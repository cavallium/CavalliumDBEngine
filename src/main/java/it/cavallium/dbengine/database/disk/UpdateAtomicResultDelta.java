package it.cavallium.dbengine.database.disk;

import io.netty5.util.Send;
import it.cavallium.dbengine.database.LLDelta;

public record UpdateAtomicResultDelta(LLDelta delta) implements UpdateAtomicResult {

	@Override
	public void close() {
		if (delta != null && delta.isAccessible()) {
			delta.close();
		}
	}
}
