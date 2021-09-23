package it.cavallium.dbengine;

public class TestLocalLLDictionary extends TestLLDictionary {

	private static final TemporaryDbGenerator GENERATOR = new LocalTemporaryDbGenerator();

	@Override
	protected TemporaryDbGenerator getTempDbGenerator() {
		return GENERATOR;
	}
}
