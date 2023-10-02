package it.cavallium.dbengine.database.disk;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter.Id;
import io.micrometer.core.instrument.Meter.Type;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.noop.NoopCounter;
import io.micrometer.core.instrument.noop.NoopTimer;

public record IteratorMetrics(Counter startedIterSeek, Counter endedIterSeek, Timer iterSeekTime,
															Counter startedIterNext, Counter endedIterNext, Timer iterNextTime) {

	public static final IteratorMetrics NO_OP = new IteratorMetrics(
			new NoopCounter(new Id("no-op", Tags.empty(), null, null, Type.COUNTER)),
			new NoopCounter(new Id("no-op", Tags.empty(), null, null, Type.COUNTER)),
			new NoopTimer(new Id("no-op", Tags.empty(), null, null, Type.TIMER)),
			new NoopCounter(new Id("no-op", Tags.empty(), null, null, Type.COUNTER)),
			new NoopCounter(new Id("no-op", Tags.empty(), null, null, Type.COUNTER)),
			new NoopTimer(new Id("no-op", Tags.empty(), null, null, Type.TIMER))
	);
}
