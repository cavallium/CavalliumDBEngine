package it.cavallium.dbengine;

public class TestLocalDictionaryMapDeepHashMap extends TestDictionaryMapDeepHashMap {

	private static final TemporaryDbGenerator GENERATOR = new LocalTemporaryDbGenerator();

	@Override
	protected TemporaryDbGenerator getTempDbGenerator() {
		return GENERATOR;
	}
}
