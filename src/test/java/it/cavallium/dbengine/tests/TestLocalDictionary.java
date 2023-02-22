package it.cavallium.dbengine.tests;

public class TestLocalDictionary extends TestDictionary {

	private static final TemporaryDbGenerator GENERATOR = new LocalTemporaryDbGenerator();

	@Override
	protected TemporaryDbGenerator getTempDbGenerator() {
		return GENERATOR;
	}
}
