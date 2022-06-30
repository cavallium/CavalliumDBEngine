package it.cavallium.dbengine.database;

import io.netty5.buffer.api.Send;
import java.util.function.Supplier;

public abstract class RangeSupplier implements DiscardingCloseable, Supplier<LLRange> {

	public static RangeSupplier of(Supplier<LLRange> supplier) {
		return new SimpleSupplier(supplier);
	}

	public static RangeSupplier of(Send<LLRange> supplier) {
		return new CopySupplier(supplier.receive());
	}

	public static RangeSupplier ofOwned(LLRange supplier) {
		return new CopySupplier(supplier);
	}

	public static RangeSupplier ofShared(LLRange supplier) {
		return new SimpleSupplier(supplier::copy);
	}

	private static final class SimpleSupplier extends RangeSupplier {

		private final Supplier<LLRange> supplier;

		public SimpleSupplier(Supplier<LLRange> supplier) {
			this.supplier = supplier;
		}

		@Override
		public LLRange get() {
			return supplier.get();
		}

		@Override
		public void close() {

		}
	}

	private static final class CopySupplier extends RangeSupplier {

		private final LLRange supplier;

		public CopySupplier(LLRange supplier) {
			this.supplier = supplier;
		}

		@Override
		public LLRange get() {
			return supplier.copy();
		}

		@Override
		public void close() {
			supplier.close();
		}
	}
}
