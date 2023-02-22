package it.cavallium.dbengine.lucene;

import static it.cavallium.dbengine.lucene.LuceneUtils.warnLuceneThread;

import it.cavallium.dbengine.utils.DBException;
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
		try {
			return directory.listAll();
		} catch (IOException e) {
			throw new DBException(e);
		}
	}

	@Override
	public void deleteFile(String name) {
		try {
			directory.deleteFile(name);
		} catch (IOException e) {
			throw new DBException(e);
		}
	}

	@Override
	public long fileLength(String name) {
		try {
			return directory.fileLength(name);
		} catch (IOException e) {
			throw new DBException(e);
		}
	}

	@Override
	public IndexOutput createOutput(String name, IOContext context) {
		LuceneUtils.checkLuceneThread();
		try {
			return new CheckIndexOutput(directory.createOutput(name, context));
		} catch (IOException e) {
			throw new DBException(e);
		}
	}

	@Override
	public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) {
		LuceneUtils.checkLuceneThread();
		try {
			return new CheckIndexOutput(directory.createTempOutput(prefix, suffix, context));
		} catch (IOException e) {
			throw new DBException(e);
		}
	}

	@Override
	public void sync(Collection<String> names) {
		LuceneUtils.checkLuceneThread();
		try {
			directory.sync(names);
		} catch (IOException e) {
			throw new DBException(e);
		}
	}

	@Override
	public void syncMetaData() {
		LuceneUtils.checkLuceneThread();
		try {
			directory.syncMetaData();
		} catch (IOException e) {
			throw new DBException(e);
		}
	}

	@Override
	public void rename(String source, String dest) {
		LuceneUtils.checkLuceneThread();
		try {
			directory.rename(source, dest);
		} catch (IOException e) {
			throw new DBException(e);
		}
	}

	@Override
	public IndexInput openInput(String name, IOContext context) {
		LuceneUtils.checkLuceneThread();
		try {
			return new CheckIndexInput(directory.openInput(name, context));
		} catch (IOException e) {
			throw new DBException(e);
		}
	}

	@Override
	public Lock obtainLock(String name) {
		LuceneUtils.checkLuceneThread();
		try {
			return directory.obtainLock(name);
		} catch (IOException e) {
			throw new DBException(e);
		}
	}

	@Override
	public void close() {
		warnLuceneThread();
		try {
			directory.close();
		} catch (IOException e) {
			throw new DBException(e);
		}
	}

	@Override
	public Set<String> getPendingDeletions() {
		try {
			return directory.getPendingDeletions();
		} catch (IOException e) {
			throw new DBException(e);
		}
	}
}
