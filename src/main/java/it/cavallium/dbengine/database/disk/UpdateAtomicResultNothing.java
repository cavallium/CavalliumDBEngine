package it.cavallium.dbengine.database.disk;

public record UpdateAtomicResultNothing() implements UpdateAtomicResult {

	@Override
	public void close() {

	}
}
