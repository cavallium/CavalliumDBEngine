package it.cavallium.dbengine.lucene;

import static it.cavallium.dbengine.lucene.LuceneUtils.warnLuceneThread;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;

public class CheckOutputDirectory extends Directory {

	private final Directory directory;

	public CheckOutputDirectory(Directory directory) {
		this.directory = directory;
	}

	@Override
	public String[] listAll() {
		return directory.listAll();
	}

	@Override
	public void deleteFile(String name) {
		directory.deleteFile(name);
	}

	@Override
	public long fileLength(String name) {
		return directory.fileLength(name);
	}

	@Override
	public IndexOutput createOutput(String name, IOContext context) {
		LuceneUtils.checkLuceneThread();
		return new CheckIndexOutput(directory.createOutput(name, context));
	}

	@Override
	public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) {
		LuceneUtils.checkLuceneThread();
		return new CheckIndexOutput(directory.createTempOutput(prefix, suffix, context));
	}

	@Override
	public void sync(Collection<String> names) {
		LuceneUtils.checkLuceneThread();
		directory.sync(names);
	}

	@Override
	public void syncMetaData() {
		LuceneUtils.checkLuceneThread();
		directory.syncMetaData();
	}

	@Override
	public void rename(String source, String dest) {
		LuceneUtils.checkLuceneThread();
		directory.rename(source, dest);
	}

	@Override
	public IndexInput openInput(String name, IOContext context) {
		LuceneUtils.checkLuceneThread();
		return new CheckIndexInput(directory.openInput(name, context));
	}

	@Override
	public Lock obtainLock(String name) {
		LuceneUtils.checkLuceneThread();
		return directory.obtainLock(name);
	}

	@Override
	public void close() {
		warnLuceneThread();
		directory.close();
	}

	@Override
	public Set<String> getPendingDeletions() {
		return directory.getPendingDeletions();
	}
}
