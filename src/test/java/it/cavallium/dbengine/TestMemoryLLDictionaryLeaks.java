package it.cavallium.dbengine;

public class TestMemoryLLDictionaryLeaks extends TestLLDictionaryLeaks {

	private static final TemporaryDbGenerator GENERATOR = new MemoryTemporaryDbGenerator();

	@Override
	protected TemporaryDbGenerator getTempDbGenerator() {
		return GENERATOR;
	}
}
