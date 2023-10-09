package it.cavallium.dbengine.database.disk.rocksdb;

import it.cavallium.dbengine.utils.SimpleResource;
import org.rocksdb.CompressionOptions;
import org.rocksdb.CompressionType;
import org.rocksdb.EnvOptions;
import org.rocksdb.Options;
import org.rocksdb.SstFileWriter;

public class LLSstFileWriter extends SimpleResource {

	private final Options options;
	private final CompressionOptions compressionOptions;
	private final SstFileWriter w;

	public LLSstFileWriter(boolean unorderedWrite) {
		this.options = new Options();
		this.compressionOptions = new CompressionOptions();
		this.w = new SstFileWriter(new EnvOptions(),
				options
						.setCompressionOptions(compressionOptions
								.setEnabled(true)
								.setMaxDictBytes(32768)
								.setZStdMaxTrainBytes(32768 * 4))
						.setCompressionType(CompressionType.ZSTD_COMPRESSION)
						.setUnorderedWrite(unorderedWrite)
		);
	}

	public SstFileWriter get() {
		return w;
	}

	@Override
	protected void onClose() {
		w.close();
		compressionOptions.close();
		options.close();
	}
}
