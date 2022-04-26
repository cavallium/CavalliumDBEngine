package it.cavallium.dbengine.database.disk;

import static it.cavallium.dbengine.database.LLUtils.isReadOnlyDirect;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.ReadableComponent;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.SafeCloseable;
import java.nio.ByteBuffer;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

public class RocksDBIterator implements SafeCloseable {

	private final RocksIterator rocksIterator;
	private final boolean allowNettyDirect;
	private final Counter startedIterSeek;
	private final Counter endedIterSeek;
	private final Timer iterSeekTime;
	private final Counter startedIterNext;
	private final Counter endedIterNext;
	private final Timer iterNextTime;

	public RocksDBIterator(RocksIterator rocksIterator,
			boolean allowNettyDirect,
			Counter startedIterSeek,
			Counter endedIterSeek,
			Timer iterSeekTime,
			Counter startedIterNext,
			Counter endedIterNext,
			Timer iterNextTime) {
		this.rocksIterator = rocksIterator;
		this.allowNettyDirect = allowNettyDirect;
		this.startedIterSeek = startedIterSeek;
		this.endedIterSeek = endedIterSeek;
		this.iterSeekTime = iterSeekTime;
		this.startedIterNext = startedIterNext;
		this.endedIterNext = endedIterNext;
		this.iterNextTime = iterNextTime;
	}

	@Override
	public void close() {
		rocksIterator.close();
	}

	public void seek(ByteBuffer seekBuf) throws RocksDBException {
		startedIterSeek.increment();
		try {
			iterSeekTime.record(() -> rocksIterator.seek(seekBuf));
		} finally {
			endedIterSeek.increment();
		}
		rocksIterator.status();
	}

	public void seek(byte[] seekArray) throws RocksDBException {
		startedIterSeek.increment();
		try {
			iterSeekTime.record(() -> rocksIterator.seek(seekArray));
		} finally {
			endedIterSeek.increment();
		}
		rocksIterator.status();
	}

	public void seekToFirst() throws RocksDBException {
		startedIterSeek.increment();
		try {
			iterSeekTime.record(rocksIterator::seekToFirst);
		} finally {
			endedIterSeek.increment();
		}
		rocksIterator.status();
	}

	public void seekToLast() throws RocksDBException {
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
	@Nullable
	public SafeCloseable seekFrom(Buffer key) {
		if (allowNettyDirect && isReadOnlyDirect(key)) {
			ByteBuffer keyInternalByteBuffer = ((ReadableComponent) key).readableBuffer();
			assert keyInternalByteBuffer.position() == 0;
			rocksIterator.seekForPrev(keyInternalByteBuffer);
			// This is useful to retain the key buffer in memory and avoid deallocations
			return key::isAccessible;
		} else {
			rocksIterator.seekForPrev(LLUtils.toArray(key));
			return null;
		}
	}

	/**
	 * Useful for forward iterations
	 */
	@Nullable
	public SafeCloseable seekTo(Buffer key) {
		if (allowNettyDirect && isReadOnlyDirect(key)) {
			ByteBuffer keyInternalByteBuffer = ((ReadableComponent) key).readableBuffer();
			assert keyInternalByteBuffer.position() == 0;
			startedIterSeek.increment();
			iterSeekTime.record(() -> rocksIterator.seek(keyInternalByteBuffer));
			endedIterSeek.increment();
			// This is useful to retain the key buffer in memory and avoid deallocations
			return key::isAccessible;
		} else {
			var array = LLUtils.toArray(key);
			startedIterSeek.increment();
			iterSeekTime.record(() -> rocksIterator.seek(array));
			endedIterSeek.increment();
			return null;
		}
	}

	public boolean isValid() {
		return rocksIterator.isValid();
	}

	public int key(ByteBuffer buffer) {
		return rocksIterator.key(buffer);
	}

	public int value(ByteBuffer buffer) {
		return rocksIterator.value(buffer);
	}

	public byte[] key() {
		return rocksIterator.key();
	}

	public byte[] value() {
		return rocksIterator.value();
	}

	public void next() throws RocksDBException {
		next(true);
	}

	public void next(boolean traceStats) throws RocksDBException {
		if (traceStats) {
			startedIterNext.increment();
			iterNextTime.record(rocksIterator::next);
			endedIterNext.increment();
		} else {
			rocksIterator.next();
		}
	}

	public void prev() throws RocksDBException {
		prev(true);
	}

	public void prev(boolean traceStats) throws RocksDBException {
		if (traceStats) {
			startedIterNext.increment();
			iterNextTime.record(rocksIterator::prev);
			endedIterNext.increment();
		} else {
			rocksIterator.prev();
		}
	}
}
