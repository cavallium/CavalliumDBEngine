package it.cavallium.dbengine.database.disk;

import io.net5.buffer.ByteBuf;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.lmdbjava.Net5ByteBufProxy;
import org.lmdbjava.Env;
import static org.lmdbjava.EnvFlags.*;

public class LLTempLMDBEnv implements Closeable {

	private static final long TWENTY_GIBIBYTES = 20L * 1024L * 1024L * 1024L;
	private static final int MAX_DATABASES = 16777216;

	private final Path tempDirectory;
	private final Env<ByteBuf> env;
	private volatile boolean closed;

	public LLTempLMDBEnv() throws IOException {
		tempDirectory = Files.createTempDirectory("lmdb");
		var envBuilder = Env.create(Net5ByteBufProxy.PROXY_NETTY)
				.setMapSize(TWENTY_GIBIBYTES)
				.setMaxDbs(MAX_DATABASES);
		//env = envBuilder.open(tempDirectory.toFile(), MDB_NOLOCK, MDB_NOSYNC, MDB_NOTLS, MDB_NORDAHEAD, MDB_WRITEMAP);
		env = envBuilder.open(tempDirectory.toFile(), MDB_NOTLS, MDB_NOSYNC, MDB_NORDAHEAD, MDB_NOMETASYNC);
	}

	public Env<ByteBuf> getEnv() {
		if (closed) {
			throw new IllegalStateException("Environment closed");
		}
		return env;
	}

	@Override
	public void close() throws IOException {
		this.closed = true;
		env.close();
		//noinspection ResultOfMethodCallIgnored
		Files.walk(tempDirectory)
				.sorted(Comparator.reverseOrder())
				.map(Path::toFile)
				.forEach(File::delete);
	}
}
