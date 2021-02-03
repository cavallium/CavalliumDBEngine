package it.cavallium.dbengine.lucene;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.StampedLock;
import org.warp.commonutils.concurrency.atomicity.Atomic;
import reactor.core.Disposable;

@Atomic
public class ScheduledTaskLifecycle {

	private final StampedLock lock;
	private final ConcurrentHashMap<Disposable, Object> tasks = new ConcurrentHashMap<>();

	public ScheduledTaskLifecycle() {
		this.lock = new StampedLock();
	}

	/**
	 * Register a scheduled task
	 */
	public void registerScheduledTask(Disposable task) {
		this.tasks.put(task, new Object());
	}

	/**
	 * Mark this task as running.
	 * After calling this method, please call {@method endScheduledTask} inside a finally block!
	 */
	public void startScheduledTask() {
		this.lock.readLock();
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
		for (var task : tasks.keySet()) {
			task.dispose();
		}
		for (var task : tasks.keySet()) {
			while (!task.isDisposed()) {
				try {
					//noinspection BusyWait
					Thread.sleep(500);
				} catch (InterruptedException ex) {
					throw new RuntimeException(ex);
				}
			}
		}

		// Acquire a write lock to wait all tasks to end
		lock.unlockWrite(lock.writeLock());
	}
}
