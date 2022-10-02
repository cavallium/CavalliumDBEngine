package it.cavallium.dbengine.database.disk.rocksdb;

import static it.cavallium.dbengine.database.LLUtils.isReadOnlyDirect;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.netty5.buffer.Buffer;
import io.netty5.buffer.BufferComponent;
import io.netty5.buffer.Drop;
import io.netty5.buffer.Owned;
import io.netty5.buffer.internal.ResourceSupport;
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
	private Buffer min;
	private Buffer max;
	private final boolean allowNettyDirect;
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
			Buffer min,
			Buffer max,
			boolean allowNettyDirect,
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
				allowNettyDirect,
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
			Buffer min,
			Buffer max,
			boolean allowNettyDirect,
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
		this.allowNettyDirect = allowNettyDirect;
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
	public void seekFrom(Buffer key) {
		ensureOpen();
		if (allowNettyDirect && isReadOnlyDirect(key)) {
			ByteBuffer keyInternalByteBuffer = ((BufferComponent) key).readableBuffer();
			assert keyInternalByteBuffer.position() == 0;
			rocksIterator.seekForPrev(keyInternalByteBuffer);
			// This is useful to retain the key buffer in memory and avoid deallocations
			this.seekingFrom = key;
		} else {
			var keyArray = LLUtils.toArray(key);
			rocksIterator.seekForPrev(keyArray);
			// This is useful to retain the key buffer in memory and avoid deallocations
			this.seekingFrom = keyArray;
		}
	}

	/**
	 * Useful for forward iterations
	 */
	public void seekTo(Buffer key) {
		ensureOpen();
		if (allowNettyDirect && isReadOnlyDirect(key)) {
			ByteBuffer keyInternalByteBuffer = ((BufferComponent) key).readableBuffer();
			assert keyInternalByteBuffer.position() == 0;
			startedIterSeek.increment();
			iterSeekTime.record(() -> rocksIterator.seek(keyInternalByteBuffer));
			endedIterSeek.increment();
			// This is useful to retain the key buffer in memory and avoid deallocations
			this.seekingTo = key;
		} else {
			var keyArray = LLUtils.toArray(key);
			startedIterSeek.increment();
			iterSeekTime.record(() -> rocksIterator.seek(keyArray));
			endedIterSeek.increment();
			// This is useful to retain the key buffer in memory and avoid deallocations
			this.seekingTo = keyArray;
		}
	}

	public boolean isValid() {
		ensureOpen();
		return rocksIterator.isValid();
	}

	public int key(ByteBuffer buffer) {
		ensureOpen();
		return rocksIterator.key(buffer);
	}

	public int value(ByteBuffer buffer) {
		ensureOpen();
		return rocksIterator.value(buffer);
	}

	public byte[] key() {
		ensureOpen();
		return rocksIterator.key();
	}

	public byte[] value() {
		ensureOpen();
		return rocksIterator.value();
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
