package it.cavallium.dbengine.database.disk;

import io.net5.buffer.ByteBuf;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.Phaser;
import org.lmdbjava.Net5ByteBufProxy;
import org.lmdbjava.Env;
import static org.lmdbjava.EnvFlags.*;

public class LLTempLMDBEnv implements Closeable {

	private static final long TEN_MEBYBYTES = 10_485_760;
	private static final int MAX_DATABASES = 1024;

	private final Phaser resources = new Phaser(1);

	private final Path tempDirectory;
	private final Env<ByteBuf> env;

	public LLTempLMDBEnv() throws IOException {
		tempDirectory = Files.createTempDirectory("lmdb");
		var envBuilder = Env.create(Net5ByteBufProxy.PROXY_NETTY)
				.setMapSize(TEN_MEBYBYTES)
				.setMaxDbs(MAX_DATABASES);
		//env = envBuilder.open(tempDirectory.toFile(), MDB_NOLOCK, MDB_NOSYNC, MDB_NOTLS, MDB_NORDAHEAD, MDB_WRITEMAP);
		env = envBuilder.open(tempDirectory.toFile(), MDB_NOTLS, MDB_WRITEMAP, MDB_NORDAHEAD);
	}

	public Env<ByteBuf> getEnvAndIncrementRef() {
		resources.register();
		return env;
	}

	public void decrementRef() {
		resources.arriveAndDeregister();
	}

	@Override
	public void close() throws IOException {
		resources.arriveAndAwaitAdvance();

		env.close();
		//noinspection ResultOfMethodCallIgnored
		Files.walk(tempDirectory)
				.sorted(Comparator.reverseOrder())
				.map(Path::toFile)
				.forEach(File::delete);
	}
}
