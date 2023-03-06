package it.cavallium.dbengine.database.disk.rocksdb;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import it.cavallium.buffer.Buf;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.utils.SimpleResource;
import java.nio.ByteBuffer;
import org.rocksdb.AbstractSlice;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

public class RocksIteratorObj extends SimpleResource {

	private RocksIterator rocksIterator;
	private AbstractSlice<?> sliceMin;
	private AbstractSlice<?> sliceMax;
	private Buf min;
	private Buf max;
	private final Counter startedIterSeek;
	private final Counter endedIterSeek;
	private final Timer iterSeekTime;
	private final Counter startedIterNext;
	private final Counter endedIterNext;
	private final Timer iterNextTime;
	private Object seekingFrom;
	private Object seekingTo;

	public RocksIteratorObj(RocksIterator rocksIterator,
			AbstractSlice<?> sliceMin,
			AbstractSlice<?> sliceMax,
			Buf min,
			Buf max,
			Counter startedIterSeek,
			Counter endedIterSeek,
			Timer iterSeekTime,
			Counter startedIterNext,
			Counter endedIterNext,
			Timer iterNextTime) {
		this(rocksIterator,
				sliceMin,
				sliceMax,
				min,
				max,
				startedIterSeek,
				endedIterSeek,
				iterSeekTime,
				startedIterNext,
				endedIterNext,
				iterNextTime,
				null,
				null
		);
	}

	private RocksIteratorObj(RocksIterator rocksIterator,
			AbstractSlice<?> sliceMin,
			AbstractSlice<?> sliceMax,
			Buf min,
			Buf max,
			Counter startedIterSeek,
			Counter endedIterSeek,
			Timer iterSeekTime,
			Counter startedIterNext,
			Counter endedIterNext,
			Timer iterNextTime,
			Object seekingFrom,
			Object seekingTo) {
		this.sliceMin = sliceMin;
		this.sliceMax = sliceMax;
		this.min = min;
		this.max = max;
		this.rocksIterator = rocksIterator;
		this.startedIterSeek = startedIterSeek;
		this.endedIterSeek = endedIterSeek;
		this.iterSeekTime = iterSeekTime;
		this.startedIterNext = startedIterNext;
		this.endedIterNext = endedIterNext;
		this.iterNextTime = iterNextTime;
		this.seekingFrom = seekingFrom;
		this.seekingTo = seekingTo;
	}

	public void seek(ByteBuffer seekBuf) throws RocksDBException {
		ensureOpen();
		startedIterSeek.increment();
		try {
			iterSeekTime.record(() -> rocksIterator.seek(seekBuf));
		} finally {
			endedIterSeek.increment();
		}
		rocksIterator.status();
	}

	public void seek(byte[] seekArray) throws RocksDBException {
		ensureOpen();
		startedIterSeek.increment();
		try {
			iterSeekTime.record(() -> rocksIterator.seek(seekArray));
		} finally {
			endedIterSeek.increment();
		}
		rocksIterator.status();
	}

	public void seekToFirst() throws RocksDBException {
		ensureOpen();
		startedIterSeek.increment();
		try {
			iterSeekTime.record(rocksIterator::seekToFirst);
		} finally {
			endedIterSeek.increment();
		}
		rocksIterator.status();
	}

	public void seekToLast() throws RocksDBException {
		ensureOpen();
		startedIterSeek.increment();
		try {
			iterSeekTime.record(rocksIterator::seekToLast);
		} finally {
			endedIterSeek.increment();
		}
		rocksIterator.status();
	}

	/**
	 * Useful for reverse iterations
	 */
	public void seekFrom(Buf key) {
		ensureOpen();
		var keyArray = LLUtils.asArray(key);
		rocksIterator.seekForPrev(keyArray);
		// This is useful to retain the key buffer in memory and avoid deallocations
		this.seekingFrom = keyArray;
	}

	/**
	 * Useful for forward iterations
	 */
	public void seekTo(Buf key) {
		ensureOpen();
		var keyArray = LLUtils.asArray(key);
		startedIterSeek.increment();
		iterSeekTime.record(() -> rocksIterator.seek(keyArray));
		endedIterSeek.increment();
		// This is useful to retain the key buffer in memory and avoid deallocations
		this.seekingTo = keyArray;
	}

	public boolean isValid() {
		ensureOpen();
		return rocksIterator.isValid();
	}

	@Deprecated(forRemoval = true)
	public int key(ByteBuffer buffer) {
		ensureOpen();
		return rocksIterator.key(buffer);
	}

	@Deprecated(forRemoval = true)
	public int value(ByteBuffer buffer) {
		ensureOpen();
		return rocksIterator.value(buffer);
	}

	/**
	 * The returned buffer may change when calling next() or when the iterator is not valid anymore
	 */
	public byte[] key() {
		ensureOpen();
		return rocksIterator.key();
	}

	/**
	 * The returned buffer may change when calling next() or when the iterator is not valid anymore
	 */
	public byte[] value() {
		ensureOpen();
		return rocksIterator.value();
	}

	/**
	 * The returned buffer may change when calling next() or when the iterator is not valid anymore
	 */
	public Buf keyBuf() {
		return Buf.wrap(this.key());
	}

	/**
	 * The returned buffer may change when calling next() or when the iterator is not valid anymore
	 */
	public Buf valueBuf() {
		return Buf.wrap(this.value());
	}

	public void next() throws RocksDBException {
		ensureOpen();
		next(true);
	}

	public void next(boolean traceStats) throws RocksDBException {
		ensureOpen();
		if (traceStats) {
			startedIterNext.increment();
			iterNextTime.record(rocksIterator::next);
			endedIterNext.increment();
		} else {
			rocksIterator.next();
		}
	}

	public void prev() throws RocksDBException {
		ensureOpen();
		prev(true);
	}

	public void prev(boolean traceStats) throws RocksDBException {
		ensureOpen();
		if (traceStats) {
			startedIterNext.increment();
			iterNextTime.record(rocksIterator::prev);
			endedIterNext.increment();
		} else {
			rocksIterator.prev();
		}
	}

	@Override
	protected void onClose() {
		if (rocksIterator != null) {
			rocksIterator.close();
		}
		if (sliceMin != null) {
			sliceMin.close();
		}
		if (sliceMax != null) {
			sliceMax.close();
		}
	}
}
