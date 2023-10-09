package it.cavallium.dbengine.database.disk.rocksdb;

import it.cavallium.dbengine.utils.SimpleResource;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileReader;

public class LLSstFileReader extends SimpleResource {

	private final Options options;
	private final SstFileReader r;

	public LLSstFileReader(boolean checks, String filePath) throws RocksDBException {
		this.options = new Options();
		this.r = new SstFileReader(options
				.setAllowMmapReads(true)
				.setParanoidChecks(checks)
		);
		r.open(filePath);
	}

	public SstFileReader get() {
		return r;
	}

	@Override
	protected void onClose() {
		r.close();
		options.close();
	}
}
