package it.cavallium.dbengine;

public class TestMemoryDictionaryMap extends TestDictionaryMap {

	private static final TemporaryDbGenerator GENERATOR = new MemoryTemporaryDbGenerator();

	@Override
	protected TemporaryDbGenerator getTempDbGenerator() {
		return GENERATOR;
	}
}
