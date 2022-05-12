package it.cavallium.dbengine.database.disk.rocksdb;

import static it.cavallium.dbengine.database.LLUtils.isReadOnlyDirect;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.Drop;
import io.netty5.buffer.api.Owned;
import io.netty5.buffer.api.ReadableComponent;
import io.netty5.buffer.api.internal.ResourceSupport;
import it.cavallium.dbengine.database.LLUtils;
import java.nio.ByteBuffer;
import org.rocksdb.AbstractSlice;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

public class RocksIteratorObj extends ResourceSupport<RocksIteratorObj, RocksIteratorObj> {

	protected static final Drop<RocksIteratorObj> DROP = new Drop<>() {
		@Override
		public void drop(RocksIteratorObj obj) {
			if (obj.rocksIterator != null) {
				obj.rocksIterator.close();
			}
			if (obj.sliceMin != null) {
				obj.sliceMin.close();
			}
			if (obj.sliceMax != null) {
				obj.sliceMax.close();
			}
		}

		@Override
		public Drop<RocksIteratorObj> fork() {
			return this;
		}

		@Override
		public void attach(RocksIteratorObj obj) {

		}
	};

	private RocksIterator rocksIterator;
	private RocksObj<? extends AbstractSlice<?>> sliceMin;
	private RocksObj<? extends AbstractSlice<?>> sliceMax;
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
			RocksObj<? extends AbstractSlice<?>> sliceMin,
			RocksObj<? extends AbstractSlice<?>> sliceMax,
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
			RocksObj<? extends AbstractSlice<?>> sliceMin,
			RocksObj<? extends AbstractSlice<?>> sliceMax,
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
		super(DROP);
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
	public void seekFrom(Buffer key) {
		if (allowNettyDirect && isReadOnlyDirect(key)) {
			ByteBuffer keyInternalByteBuffer = ((ReadableComponent) key).readableBuffer();
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
		if (allowNettyDirect && isReadOnlyDirect(key)) {
			ByteBuffer keyInternalByteBuffer = ((ReadableComponent) key).readableBuffer();
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

	@Override
	protected void makeInaccessible() {
		this.rocksIterator = null;
		this.sliceMin = null;
		this.sliceMax = null;
		this.min = null;
		this.max = null;
		this.seekingFrom = null;
		this.seekingTo = null;
	}

	@Override
	protected RuntimeException createResourceClosedException() {
		return new IllegalStateException("Closed");
	}

	@Override
	protected Owned<RocksIteratorObj> prepareSend() {
		var rocksIterator = this.rocksIterator;
		var sliceMin = this.sliceMin;
		var sliceMax = this.sliceMax;
		var minSend = this.min != null ? this.min.send() : null;
		var maxSend = this.max != null ? this.max.send() : null;
		var seekingFrom = this.seekingFrom;
		var seekingTo = this.seekingTo;
		return drop -> {
			var instance = new RocksIteratorObj(rocksIterator,
					sliceMin,
					sliceMax,
					minSend != null ? minSend.receive() : null,
					maxSend != null ? maxSend.receive() : null,
					allowNettyDirect,
					startedIterSeek,
					endedIterSeek,
					iterSeekTime,
					startedIterNext,
					endedIterNext,
					iterNextTime,
					seekingFrom,
					seekingTo
			);
			drop.attach(instance);
			return instance;
		};
	}
}
