package it.cavallium.dbengine;

public class TestLocalLLDictionaryLeaks extends TestLLDictionaryLeaks {

	private static final TemporaryDbGenerator GENERATOR = new LocalTemporaryDbGenerator();

	@Override
	protected TemporaryDbGenerator getTempDbGenerator() {
		return GENERATOR;
	}
}
