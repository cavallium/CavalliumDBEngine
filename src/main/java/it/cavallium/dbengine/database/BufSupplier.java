package it.cavallium.dbengine.database;

import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.Send;
import java.util.function.Supplier;

public abstract class BufSupplier implements SafeCloseable, Supplier<Buffer> {

	public static BufSupplier of(Supplier<Buffer> supplier) {
		return new SimpleBufSupplier(supplier);
	}

	public static BufSupplier of(Send<Buffer> supplier) {
		return new CopyBufSupplier(supplier.receive());
	}

	public static BufSupplier ofOwned(Buffer supplier) {
		return new CopyBufSupplier(supplier);
	}

	public static BufSupplier ofShared(Buffer supplier) {
		return new SimpleBufSupplier(supplier::copy);
	}

	private static final class SimpleBufSupplier extends BufSupplier {

		private final Supplier<Buffer> supplier;

		public SimpleBufSupplier(Supplier<Buffer> supplier) {
			this.supplier = supplier;
		}

		@Override
		public Buffer get() {
			return supplier.get();
		}

		@Override
		public void close() {

		}
	}

	private static final class CopyBufSupplier extends BufSupplier {

		private final Buffer supplier;

		public CopyBufSupplier(Buffer supplier) {
			this.supplier = supplier;
		}

		@Override
		public Buffer get() {
			return supplier.copy();
		}

		@Override
		public void close() {
			supplier.close();
		}
	}
}
