package it.cavallium.dbengine;

public class TestLocalDictionaryMap extends TestDictionaryMap {

	private static final TemporaryDbGenerator GENERATOR = new LocalTemporaryDbGenerator();

	@Override
	protected TemporaryDbGenerator getTempDbGenerator() {
		return GENERATOR;
	}
}
