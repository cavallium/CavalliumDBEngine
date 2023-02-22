package it.cavallium.dbengine.tests;

public class TestMemorySingletons extends TestSingletons {

	private static final TemporaryDbGenerator GENERATOR = new MemoryTemporaryDbGenerator();

	@Override
	protected TemporaryDbGenerator getTempDbGenerator() {
		return GENERATOR;
	}
}
