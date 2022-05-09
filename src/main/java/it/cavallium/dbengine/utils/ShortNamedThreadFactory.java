/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package it.cavallium.dbengine.utils;


import java.util.Locale;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import org.jetbrains.annotations.NotNull;

/**
 * A default {@link ThreadFactory} implementation that accepts the name prefix
 * of the created threads as a constructor argument. Otherwise, this factory
 * yields the same semantics as the thread factory returned by
 * {@link Executors#defaultThreadFactory()}.
 */
public class ShortNamedThreadFactory implements ThreadFactory {

	private static int POOL_NUMBERS_COUNT = 50;
	private static final AtomicInteger[] threadPoolNumber = new AtomicInteger[POOL_NUMBERS_COUNT];
	static {
		for (int i = 0; i < threadPoolNumber.length; i++) {
			threadPoolNumber[i] = new AtomicInteger(1);
		}
	}
	private ThreadGroup group;
	private boolean daemon;
	private final AtomicInteger threadNumber = new AtomicInteger(1);
	private static final String NAME_PATTERN = "%s-%d";
	private final String threadNamePrefix;

	/**
	 * Creates a new {@link ShortNamedThreadFactory} instance
	 *
	 * @param threadNamePrefix the name prefix assigned to each thread created.
	 */
	public ShortNamedThreadFactory(String threadNamePrefix) {
		group = Thread.currentThread().getThreadGroup();
		this.threadNamePrefix = String.format(Locale.ROOT, NAME_PATTERN,
				checkPrefix(threadNamePrefix), threadPoolNumber[(threadNamePrefix.hashCode() % POOL_NUMBERS_COUNT / 2) + POOL_NUMBERS_COUNT / 2].getAndIncrement());
	}

	public ShortNamedThreadFactory withGroup(ThreadGroup threadGroup) {
		this.group = threadGroup;
		return this;
	}

	public ShortNamedThreadFactory setDaemon(boolean daemon) {
		this.daemon = daemon;
		return this;
	}

	private static String checkPrefix(String prefix) {
		return prefix == null || prefix.length() == 0 ? "Unnamed" : prefix;
	}

	/**
	 * Creates a new {@link Thread}
	 *
	 * @see ThreadFactory#newThread(Runnable)
	 */
	@Override
	public Thread newThread(@NotNull Runnable r) {
		final Thread t = new Thread(group, r, String.format(Locale.ROOT, "%s-%d",
				this.threadNamePrefix, threadNumber.getAndIncrement()), 0);
		t.setDaemon(daemon);
		t.setPriority(Thread.NORM_PRIORITY);
		return t;
	}

}
