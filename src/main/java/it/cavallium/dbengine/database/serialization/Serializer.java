package it.cavallium.dbengine.database.serialization;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.PooledByteBufAllocator;
import java.nio.charset.StandardCharsets;
import org.jetbrains.annotations.NotNull;

public interface Serializer<A, B> {

	@NotNull A deserialize(@NotNull B serialized);

	@NotNull B serialize(@NotNull A deserialized);

	Serializer<ByteBuf, ByteBuf> NOOP_SERIALIZER = new Serializer<>() {
		@Override
		public @NotNull ByteBuf deserialize(@NotNull ByteBuf serialized) {
			try {
				return serialized.retainedSlice();
			} finally {
				serialized.release();
			}
		}

		@Override
		public @NotNull ByteBuf serialize(@NotNull ByteBuf deserialized) {
			try {
				return deserialized.retainedSlice();
			} finally {
				deserialized.release();
			}
		}
	};

	static Serializer<ByteBuf, ByteBuf> noop() {
		return NOOP_SERIALIZER;
	}

	static Serializer<String, ByteBuf> utf8(ByteBufAllocator allocator) {
		return new Serializer<>() {
			@Override
			public @NotNull String deserialize(@NotNull ByteBuf serialized) {
				try {
					var result = serialized.toString(StandardCharsets.UTF_8);
					serialized.readerIndex(serialized.writerIndex());
					return result;
				} finally {
					serialized.release();
				}
			}

			@Override
			public @NotNull ByteBuf serialize(@NotNull String deserialized) {
				// UTF-8 uses max. 3 bytes per char, so calculate the worst case.
				ByteBuf buf = allocator.buffer(ByteBufUtil.utf8MaxBytes(deserialized));
				ByteBufUtil.writeUtf8(buf, deserialized);
				return buf;
			}
		};
	}
}
