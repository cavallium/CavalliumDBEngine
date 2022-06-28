package it.cavallium.dbengine.lucene.searcher;

import it.cavallium.dbengine.client.query.current.data.TotalHitsCount;
import org.jetbrains.annotations.NotNull;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;

public class CountedFlux<T> extends Flux<T> {

	private final TotalHitsCount totalHitsCount;
	private final Flux<? extends T> flux;

	private CountedFlux(TotalHitsCount totalHitsCount, Flux<? extends T> flux) {
		this.totalHitsCount = totalHitsCount;
		this.flux = flux;
	}

	public static <T> CountedFlux<T> of(TotalHitsCount totalHitsCount, Flux<? extends T> flux) {
		return new CountedFlux<>(totalHitsCount, flux);
	}

	public TotalHitsCount totalHitsCount() {
		return totalHitsCount;
	}

	@Override
	public void subscribe(@NotNull CoreSubscriber<? super T> actual) {
		flux.subscribe(actual);
	}
}
