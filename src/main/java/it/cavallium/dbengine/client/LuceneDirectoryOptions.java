package it.cavallium.dbengine.client;

import it.cavallium.dbengine.lucene.directory.RocksdbDirectory;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.misc.store.DirectIODirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Constants;

public sealed interface LuceneDirectoryOptions {

	Directory createLuceneDirectory(String directoryName) throws IOException;

	Optional<Path> getManagedPath();

	record ByteBuffersDirectory() implements LuceneDirectoryOptions {

		@Override
		public Directory createLuceneDirectory(String directoryName) {
			return new org.apache.lucene.store.ByteBuffersDirectory();
		}

		@Override
		public Optional<Path> getManagedPath() {
			return Optional.empty();
		}
	}

	record MemoryMappedFSDirectory(Path managedPath) implements StandardFSDirectoryOptions {

		@Override
		public FSDirectory createLuceneDirectory(String directoryName) throws IOException {
			return FSDirectory.open(managedPath.resolve(directoryName + ".lucene.db"));
		}
	}

	record NIOFSDirectory(Path managedPath) implements StandardFSDirectoryOptions {

		@Override
		public FSDirectory createLuceneDirectory(String directoryName) throws IOException {
			return org.apache.lucene.store.NIOFSDirectory.open(managedPath.resolve(directoryName + ".lucene.db"));
		}
	}

	record DirectIOFSDirectory(StandardFSDirectoryOptions delegate, Optional<Integer> mergeBufferSize,
														 Optional<Long> minBytesDirect) implements LuceneDirectoryOptions {

		private static final Logger logger = LogManager.getLogger(DirectIOFSDirectory.class);

		@Override
		public Directory createLuceneDirectory(String directoryName) throws IOException {
			var delegateDirectory = delegate.createLuceneDirectory(directoryName);
			if (Constants.LINUX || Constants.MAC_OS_X) {
				try {
					int mergeBufferSize = mergeBufferSize().orElse(DirectIODirectory.DEFAULT_MERGE_BUFFER_SIZE);
					long minBytesDirect = minBytesDirect().orElse(DirectIODirectory.DEFAULT_MIN_BYTES_DIRECT);
					return new DirectIODirectory(delegateDirectory, mergeBufferSize, minBytesDirect);
				} catch (UnsupportedOperationException ex) {
					logger.warn("Failed to open FSDirectory with DIRECT flag", ex);
					return delegateDirectory;
				}
			} else {
				logger.warn("Failed to open FSDirectory with DIRECT flag because the operating system is Windows");
				return delegateDirectory;
			}
		}

		@Override
		public Optional<Path> getManagedPath() {
			return delegate.getManagedPath();
		}
	}

	record RocksDBDirectory(Path managedPath) implements PathDirectoryOptions {

		@Override
		public Directory createLuceneDirectory(String directoryName) throws IOException {
			return new RocksdbDirectory(managedPath.resolve(directoryName + ".lucene.db"));
		}
	}

	record NRTCachingDirectory(LuceneDirectoryOptions delegate, long maxMergeSizeBytes, long maxCachedBytes) implements
			LuceneDirectoryOptions {

		@Override
		public Directory createLuceneDirectory(String directoryName) throws IOException {
			var delegateDirectory = delegate.createLuceneDirectory(directoryName);
			return new org.apache.lucene.store.NRTCachingDirectory(delegateDirectory,
					maxMergeSizeBytes / 1024D / 1024D,
					maxCachedBytes / 1024D / 1024D
			);
		}

		@Override
		public Optional<Path> getManagedPath() {
			return delegate.getManagedPath();
		}
	}

	sealed interface StandardFSDirectoryOptions extends PathDirectoryOptions {

		@Override
		FSDirectory createLuceneDirectory(String directoryName) throws IOException;
	}

	sealed interface PathDirectoryOptions extends LuceneDirectoryOptions {

		Path managedPath();

		@Override
		default Optional<Path> getManagedPath() {
			return Optional.of(managedPath());
		}
	}
}
