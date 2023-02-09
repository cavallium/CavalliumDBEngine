package it.cavallium.dbengine.lucene;

import static it.cavallium.dbengine.lucene.LuceneUtils.warnLuceneThread;

import java.io.IOException;
import org.apache.lucene.store.IndexOutput;

public class CheckIndexOutput extends IndexOutput {

	private final IndexOutput output;

	public CheckIndexOutput(IndexOutput output) {
		super(output.toString(), output.getName());
		this.output = output;
	}

	private static void checkThread() {
		assert LuceneUtils.isLuceneThread();
	}

	@Override
	public void close() {
		warnLuceneThread();
		output.close();
	}

	@Override
	public long getFilePointer() {
		checkThread();
		return output.getFilePointer();
	}

	@Override
	public long getChecksum() {
		checkThread();
		return output.getChecksum();
	}

	@Override
	public void writeByte(byte b) {
		checkThread();
		output.writeByte(b);
	}

	@Override
	public void writeBytes(byte[] b, int offset, int length) {
		checkThread();
		output.writeBytes(b, offset, length);
	}

	@Override
	public String getName() {
		return output.getName();
	}

	@Override
	public String toString() {
		return output.toString();
	}
}
