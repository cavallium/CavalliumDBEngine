package it.cavallium.dbengine.lucene;

import org.jetbrains.annotations.NotNull;

public class LuceneThread extends Thread {

	public LuceneThread(ThreadGroup group, @NotNull Runnable runnable, String name, int stackSize) {
		super(group, runnable, name, stackSize);
	}
}
