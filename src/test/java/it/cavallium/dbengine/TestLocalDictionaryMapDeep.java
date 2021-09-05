package it.cavallium.dbengine;

public class TestLocalDictionaryMapDeep extends TestDictionaryMapDeep {

	private static final TemporaryDbGenerator GENERATOR = new LocalTemporaryDbGenerator();

	@Override
	protected TemporaryDbGenerator getTempDbGenerator() {
		return GENERATOR;
	}
}
