package it.cavallium.dbengine.tests;

public class TestLocalDictionaryMap extends TestDictionaryMap {

	private static final TemporaryDbGenerator GENERATOR = new LocalTemporaryDbGenerator();

	@Override
	protected TemporaryDbGenerator getTempDbGenerator() {
		return GENERATOR;
	}
}
