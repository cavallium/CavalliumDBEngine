package it.cavallium.dbengine.lucene;

import java.io.IOException;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.MergePolicy.OneMerge;

public class LuceneConcurrentMergeScheduler extends ConcurrentMergeScheduler {

	public LuceneConcurrentMergeScheduler() {
		super();
	}

	@Override
	protected synchronized MergeThread getMergeThread(MergeSource mergeSource, OneMerge merge) throws IOException {
		final MergeThread thread = new LuceneMergeThread(mergeSource, merge);
		thread.setDaemon(true);
		thread.setName("lucene-merge-" + mergeThreadCount++);
		return thread;
	}

	public class LuceneMergeThread extends MergeThread {

		/**
		 * Sole constructor.
		 *
		 * @param mergeSource
		 * @param merge
		 */
		public LuceneMergeThread(MergeSource mergeSource, OneMerge merge) {
			super(mergeSource, merge);
		}
	}
}
