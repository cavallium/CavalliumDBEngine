package it.cavallium.dbengine.database;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.jetbrains.annotations.Nullable;
import org.warp.commonutils.log.Logger;
import org.warp.commonutils.log.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;

public abstract class BlockingFluxIterable<T> {

	private static final Logger logger = LoggerFactory.getLogger(BlockingFluxIterable.class);
	private final String name;
	private AtomicBoolean cancelled = new AtomicBoolean(false);

	public BlockingFluxIterable(String name) {
		this.name = name;
	}

	public Flux<T> generate(Scheduler scheduler) {
		logger.trace("Generating iterable flux {}", this.name);
		AtomicBoolean alreadyInitialized = new AtomicBoolean(false);
		AtomicLong requests = new AtomicLong(0);
		Semaphore availableRequests = new Semaphore(0);
		return Flux
				.<T>create(sink -> {
					sink.onRequest(n -> {
						requests.addAndGet(n);
						availableRequests.release();
					});
					sink.onDispose(() -> {
						cancelled.set(true);
						availableRequests.release();
					});

					scheduler.schedule(() -> {
						logger.trace("Starting iterable flux {}", this.name);
						try {
							try {
								loop:
								while (true) {
									availableRequests.acquireUninterruptibly();
									var remainingRequests = requests.getAndSet(0);
									if (cancelled.get()) {
										break;
									}

									while (remainingRequests-- > 0) {
										if (alreadyInitialized.compareAndSet(false, true)) {
											this.onStartup();
										}

										T next = onNext();
										if (next == null) {
											break loop;
										}
										sink.next(next);
									}
								}
							} catch (InterruptedException ex) {
								sink.error(ex);
							} finally {
								if (alreadyInitialized.get()) {
									onTerminate();
								}
							}
						} finally {
							sink.complete();
						}
					});
				});
	}

	public Flux<T> generateNonblocking(Scheduler scheduler, int rateLimit) {
		logger.trace("Generating nonblocking iterable flux {}", this.name);
		AtomicBoolean alreadyInitialized = new AtomicBoolean(false);
		final Object lock = new Object();
		return Flux
				.<T>create(sink -> {
					sink.onRequest(n -> {
						if (n > rateLimit || n < 1) {
							sink.error(new IndexOutOfBoundsException("Requests must be <= " + rateLimit));
						} else {
							scheduler.schedule(() -> {
								synchronized (lock) {
									if (cancelled.get()) {
										return;
									}
									int remaining = (int) n;
									try {
										T next;
										do {
											if (alreadyInitialized.compareAndSet(false, true)) {
												this.onStartup();
											}
											next = this.onNext();
											if (next != null) {
												sink.next(next);
											} else {
												cancelled.set(true);
												sink.complete();
											}
										} while (next != null && --remaining > 0 && !cancelled.get());
									} catch (InterruptedException e) {
										sink.error(e);
									}
								}
							});
						}
					});
					sink.onCancel(() -> {
					});
					sink.onDispose(() -> {
						cancelled.set(true);
						scheduler.schedule(() -> {
							synchronized (lock) {
								if (alreadyInitialized.get()) {
									this.onTerminate();
								}
							}
						});
					});
				})
				.limitRate(rateLimit);
	}

	public abstract void onStartup();

	public abstract void onTerminate();

	@Nullable
	public abstract T onNext() throws InterruptedException;

	protected boolean isCancelled() {
		return cancelled.get();
	}
}
