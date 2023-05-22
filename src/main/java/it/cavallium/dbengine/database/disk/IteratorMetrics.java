package it.cavallium.dbengine.database.disk;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;

public record IteratorMetrics(Counter startedIterSeek, Counter endedIterSeek, Timer iterSeekTime,
															Counter startedIterNext, Counter endedIterNext, Timer iterNextTime) {}
