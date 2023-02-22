package it.cavallium.dbengine.tests;

public class TestMemoryLLDictionary extends TestLLDictionary {

	private static final TemporaryDbGenerator GENERATOR = new MemoryTemporaryDbGenerator();

	@Override
	protected TemporaryDbGenerator getTempDbGenerator() {
		return GENERATOR;
	}
}
