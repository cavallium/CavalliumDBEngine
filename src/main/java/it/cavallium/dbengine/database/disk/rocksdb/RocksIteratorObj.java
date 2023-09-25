package it.cavallium.dbengine.database.disk.rocksdb;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import it.cavallium.buffer.Buf;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.disk.IteratorMetrics;
import it.cavallium.dbengine.utils.SimpleResource;
import java.nio.ByteBuffer;
import org.rocksdb.AbstractSlice;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

public class RocksIteratorObj extends SimpleResource {

	private LLReadOptions readOptions;
	private final RocksIterator rocksIterator;
	private final Counter startedIterSeek;
	private final Counter endedIterSeek;
	private final Timer iterSeekTime;
	private final Counter startedIterNext;
	private final Counter endedIterNext;
	private final Timer iterNextTime;
	private byte[] seekingFrom;
	private byte[] seekingTo;

	RocksIteratorObj(RocksIterator rocksIterator,
			LLReadOptions readOptions, IteratorMetrics iteratorMetrics) {
		super(rocksIterator::close);
		this.readOptions = readOptions;
		this.rocksIterator = rocksIterator;
		this.startedIterSeek = iteratorMetrics.startedIterSeek();
		this.startedIterNext = iteratorMetrics.startedIterNext();
		this.iterSeekTime = iteratorMetrics.iterSeekTime();
		this.endedIterNext = iteratorMetrics.endedIterNext();
		this.endedIterSeek = iteratorMetrics.endedIterSeek();
		this.iterNextTime = iteratorMetrics.iterNextTime();
	}

	public synchronized void seek(ByteBuffer seekBuf) throws RocksDBException {
		ensureOpen();
		startedIterSeek.increment();
		try {
			iterSeekTime.record(() -> rocksIterator.seek(seekBuf));
		} finally {
			endedIterSeek.increment();
		}
		rocksIterator.status();
	}

	public synchronized void seek(byte[] seekArray) throws RocksDBException {
		ensureOpen();
		startedIterSeek.increment();
		try {
			iterSeekTime.record(() -> rocksIterator.seek(seekArray));
		} finally {
			endedIterSeek.increment();
		}
		rocksIterator.status();
	}

	public synchronized void seekToFirst() throws RocksDBException {
		ensureOpen();
		startedIterSeek.increment();
		try {
			iterSeekTime.record(rocksIterator::seekToFirst);
		} finally {
			endedIterSeek.increment();
		}
		rocksIterator.status();
	}

	public synchronized void seekToLast() throws RocksDBException {
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
	public synchronized void seekFrom(Buf key) throws RocksDBException {
		ensureOpen();
		var keyArray = LLUtils.asArray(key);
		try {
			rocksIterator.seekForPrev(keyArray);
		} finally {
		}
		rocksIterator.status();
		// This is useful to retain the key buffer in memory and avoid deallocations
		this.seekingFrom = keyArray;
	}

	/**
	 * Useful for forward iterations
	 */
	public synchronized void seekTo(Buf key) throws RocksDBException {
		ensureOpen();
		var keyArray = LLUtils.asArray(key);
		startedIterSeek.increment();
		try {
			iterSeekTime.record(() -> rocksIterator.seek(keyArray));
		} finally {
			endedIterSeek.increment();
		}
		rocksIterator.status();
		// This is useful to retain the key buffer in memory and avoid deallocations
		this.seekingTo = keyArray;
	}

	public synchronized boolean isValid() {
		ensureOpen();
		return rocksIterator.isValid();
	}

	@Deprecated(forRemoval = true)
	public synchronized int key(ByteBuffer buffer) {
		ensureOpen();
		return rocksIterator.key(buffer);
	}

	@Deprecated(forRemoval = true)
	public synchronized int value(ByteBuffer buffer) {
		ensureOpen();
		return rocksIterator.value(buffer);
	}

	/**
	 * The returned buffer may change when calling next() or when the iterator is not valid anymore
	 */
	public synchronized byte[] key() {
		ensureOpen();
		return rocksIterator.key();
	}

	/**
	 * The returned buffer may change when calling next() or when the iterator is not valid anymore
	 */
	public synchronized byte[] value() {
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
		next(true);
	}

	public synchronized void next(boolean traceStats) throws RocksDBException {
		ensureOpen();
		if (traceStats) {
			startedIterNext.increment();
			iterNextTime.record(rocksIterator::next);
			endedIterNext.increment();
		} else {
			rocksIterator.next();
		}
		rocksIterator.status();
	}

	public void prev() throws RocksDBException {
		prev(true);
	}

	public synchronized void prev(boolean traceStats) throws RocksDBException {
		ensureOpen();
		if (traceStats) {
			startedIterNext.increment();
			iterNextTime.record(rocksIterator::prev);
			endedIterNext.increment();
		} else {
			rocksIterator.prev();
		}
		rocksIterator.status();
	}

	@Override
	protected synchronized void onClose() {
		if (rocksIterator != null) {
			rocksIterator.close();
		}
		seekingFrom = null;
		seekingTo = null;
		readOptions = null;
	}
}
