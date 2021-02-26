package it.cavallium.dbengine.database;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.jetbrains.annotations.Nullable;
import org.warp.commonutils.log.Logger;
import org.warp.commonutils.log.LoggerFactory;
import reactor.core.publisher.Flux;

public abstract class BlockingFluxIterable<T> {

	private static final Logger logger = LoggerFactory.getLogger(BlockingFluxIterable.class);
	private final String name;
	private AtomicBoolean alreadyInitialized = new AtomicBoolean(false);
	private AtomicLong requests = new AtomicLong(0);
	private Semaphore availableRequests = new Semaphore(0);
	private AtomicBoolean cancelled = new AtomicBoolean(false);

	public BlockingFluxIterable(String name) {
		this.name = name;
	}

	public Flux<T> generate() {
		logger.trace("Generating iterable flux {}", this.name);
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

					new Thread(() -> {
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
					}, "blocking-flux-iterable").start();
				});
	}

	public abstract void onStartup();

	public abstract void onTerminate();

	@Nullable
	public abstract T onNext() throws InterruptedException;

	protected boolean isCancelled() {
		return cancelled.get();
	}
}
