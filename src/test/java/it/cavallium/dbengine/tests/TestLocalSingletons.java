package it.cavallium.dbengine.tests;

public class TestLocalSingletons extends TestSingletons {

	private static final TemporaryDbGenerator GENERATOR = new LocalTemporaryDbGenerator();

	@Override
	protected TemporaryDbGenerator getTempDbGenerator() {
		return GENERATOR;
	}
}
