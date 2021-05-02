package it.cavallium.dbengine.database.disk;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import it.cavallium.dbengine.database.LLRange;
import it.cavallium.dbengine.database.LLUtils;
import it.cavallium.dbengine.database.collections.DatabaseMapDictionaryDeep;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksMutableObject;
import reactor.core.publisher.Flux;
import static io.netty.buffer.Unpooled.*;

public abstract class LLLocalGroupedReactiveRocksIterator<T> {

	private final RocksDB db;
	private final ByteBufAllocator alloc;
	private final ColumnFamilyHandle cfh;
	private final int prefixLength;
	private final LLRange range;
	private final ReadOptions readOptions;
	private final boolean canFillCache;
	private final boolean readValues;

	public LLLocalGroupedReactiveRocksIterator(RocksDB db, ByteBufAllocator alloc, ColumnFamilyHandle cfh,
			int prefixLength,
			LLRange range,
			ReadOptions readOptions,
			boolean canFillCache,
			boolean readValues) {
		this.db = db;
		this.alloc = alloc;
		this.cfh = cfh;
		this.prefixLength = prefixLength;
		this.range = range;
		this.readOptions = readOptions;
		this.canFillCache = canFillCache;
		this.readValues = readValues;
	}


	public Flux<List<T>> flux() {
		return Flux
				.generate(() -> {
					var readOptions = new ReadOptions(this.readOptions);
					readOptions.setFillCache(canFillCache && range.hasMin() && range.hasMax());
					return LLLocalDictionary.getRocksIterator(readOptions, range.retain(), db, cfh);
				}, (tuple, sink) -> {
					range.retain();
					try {
						var rocksIterator = tuple.getT1();
						ObjectArrayList<T> values = new ObjectArrayList<>();
						ByteBuf firstGroupKey = null;
						try {
							while (rocksIterator.isValid()) {
								ByteBuf key = LLUtils.readDirectNioBuffer(alloc, rocksIterator::key);
								try {
									if (firstGroupKey == null) {
										firstGroupKey = key.retain();
									} else if (!ByteBufUtil.equals(firstGroupKey, firstGroupKey.readerIndex(), key, key.readerIndex(), prefixLength)) {
										break;
									}
									ByteBuf value;
									if (readValues) {
										value = LLUtils.readDirectNioBuffer(alloc, rocksIterator::value);
									} else {
										value = DatabaseMapDictionaryDeep.EMPTY_BYTES;
									}
									try {
										rocksIterator.next();
										T entry = getEntry(key.retain(), value.retain());
										values.add(entry);
									} finally {
										value.release();
									}
								} finally {
									key.release();
								}
							}
						} finally {
							if (firstGroupKey != null) {
								firstGroupKey.release();
							}
						}
						if (!values.isEmpty()) {
							sink.next(values);
						} else {
							sink.complete();
						}
						return tuple;
					} finally {
						range.release();
					}
				}, tuple -> {
					var rocksIterator = tuple.getT1();
					rocksIterator.close();
					tuple.getT2().release();
					tuple.getT3().release();
				});
	}

	public abstract T getEntry(ByteBuf key, ByteBuf value);

	public void release() {
		range.release();
	}
}
