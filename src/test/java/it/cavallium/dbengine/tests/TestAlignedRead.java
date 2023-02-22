package it.cavallium.dbengine.tests;

import it.cavallium.dbengine.lucene.DirectNIOFSDirectory;
import it.cavallium.dbengine.lucene.LuceneUtils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestAlignedRead {
	@Test
	public void testAlignment() {
		Assertions.assertEquals(0, LuceneUtils.alignUnsigned(0, true));
		Assertions.assertEquals(0, LuceneUtils.alignUnsigned(0, false));
		Assertions.assertEquals(4096, LuceneUtils.alignUnsigned(1, true));
		Assertions.assertEquals(0, LuceneUtils.alignUnsigned(1, false));
		Assertions.assertEquals(4096, LuceneUtils.alignUnsigned(4096, true));
		Assertions.assertEquals(4096, LuceneUtils.alignUnsigned(4096, false));
	}
}
