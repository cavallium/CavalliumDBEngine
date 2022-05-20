package it.cavallium.dbengine.database.disk;

public record UpdateAtomicResultBinaryChanged(boolean changed) implements UpdateAtomicResult {

	@Override
	public void close() {

	}
}
