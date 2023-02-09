package it.cavallium.dbengine.lucene;

import java.io.IOException;
import java.nio.file.Path;
import java.util.OptionalLong;
import org.apache.lucene.misc.store.DirectIODirectory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;

public class AlwaysDirectIOFSDirectory extends DirectIODirectory {

	public AlwaysDirectIOFSDirectory(Path path, int mergeBufferSize, long minBytesDirect) {
		super(FSDirectory.open(path), mergeBufferSize, minBytesDirect);
	}

	public AlwaysDirectIOFSDirectory(Path path) {
		super(FSDirectory.open(path));
	}

	@Override
	protected boolean useDirectIO(String name, IOContext context, OptionalLong fileLength) {
		return true;
	}
}
