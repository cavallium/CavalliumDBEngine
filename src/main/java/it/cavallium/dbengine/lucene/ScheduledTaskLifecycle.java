package it.cavallium.dbengine.lucene;

import java.nio.channels.ClosedChannelException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.StampedLock;
import org.warp.commonutils.concurrency.atomicity.Atomic;
import reactor.core.Disposable;

@Atomic
public class ScheduledTaskLifecycle {

	private final StampedLock lock;
	private volatile boolean cancelled = false;

	public ScheduledTaskLifecycle() {
		this.lock = new StampedLock();
	}

	/**
	 * Mark this task as running.
	 * After calling this method, please call {@method endScheduledTask} inside a finally block!
	 */
	public void startScheduledTask() {
		if (cancelled) {
			throw new IllegalStateException("Already closed");
		}
		this.lock.readLock();
	}

	/**
	 * Mark this task as running.
	 * After calling this method, please call {@method endScheduledTask} inside a finally block!
	 * @return false if failed
	 */
	public boolean tryStartScheduledTask() {
		if (cancelled) {
			return false;
		}
		this.lock.readLock();
		return true;
	}

	/**
	 * Mark this task as ended. Must be called after {@method startScheduledTask}
	 */
	public void endScheduledTask() {
		this.lock.tryUnlockRead();
	}

	/**
	 * Cancel all scheduled tasks and wait all running methods to finish
	 */
	public void cancelAndWait() {
		cancelled = true;

		// Acquire a write lock to wait all tasks to end
		lock.unlockWrite(lock.writeLock());
	}

	public boolean isCancelled() {
		return cancelled;
	}
}
