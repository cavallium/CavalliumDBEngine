package it.cavallium.dbengine.database.disk;

import it.cavallium.dbengine.lucene.LuceneThread;
import it.cavallium.dbengine.utils.ShortNamedThreadFactory;
import java.util.Locale;
import org.jetbrains.annotations.NotNull;

public class LuceneThreadFactory extends ShortNamedThreadFactory {

	/**
	 * Creates a new {@link ShortNamedThreadFactory} instance
	 *
	 * @param threadNamePrefix the name prefix assigned to each thread created.
	 */
	public LuceneThreadFactory(String threadNamePrefix) {
		super(threadNamePrefix);
	}

	@Override
	public Thread newThread(@NotNull Runnable r) {
		final Thread t = new LuceneThread(group, r, String.format(Locale.ROOT, "%s-%d",
				this.threadNamePrefix, threadNumber.getAndIncrement()), 0);
		t.setDaemon(daemon);
		t.setPriority(Thread.NORM_PRIORITY);
		return t;
	}
}
