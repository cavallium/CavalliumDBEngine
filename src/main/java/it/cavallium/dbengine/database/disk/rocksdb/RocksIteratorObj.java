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

	public void next(boolean traceStats) {
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

	public void prev(boolean traceStats) {
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
		seekingFrom = null;
		seekingTo = null;
		readOptions = null;
	}
}
