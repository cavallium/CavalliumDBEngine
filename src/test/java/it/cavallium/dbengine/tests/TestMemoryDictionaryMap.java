package it.cavallium.dbengine.tests;

public class TestMemoryDictionaryMap extends TestDictionaryMap {

	private static final TemporaryDbGenerator GENERATOR = new MemoryTemporaryDbGenerator();

	@Override
	protected TemporaryDbGenerator getTempDbGenerator() {
		return GENERATOR;
	}
}
