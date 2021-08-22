package it.cavallium.dbengine.database.serialization;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.PooledByteBufAllocator;
import java.io.NotSerializableException;
import java.nio.charset.StandardCharsets;
import org.jetbrains.annotations.NotNull;

@SuppressWarnings("unused")
public interface SerializerFixedBinaryLength<A, B> extends Serializer<A, B> {

	int getSerializedBinaryLength();

	static SerializerFixedBinaryLength<ByteBuf, ByteBuf> noop(int length) {
		return new SerializerFixedBinaryLength<>() {
			@Override
			public @NotNull ByteBuf deserialize(@NotNull ByteBuf serialized) {
				try {
					if (serialized.readableBytes() != getSerializedBinaryLength()) {
						throw new IllegalArgumentException(
								"Fixed serializer with " + getSerializedBinaryLength() + " bytes has tried to deserialize an element with "
										+ serialized.readableBytes() + " bytes instead");
					}
					return serialized.retain();
				} finally {
					serialized.release();
				}
			}

			@Override
			public @NotNull ByteBuf serialize(@NotNull ByteBuf deserialized) {
				ByteBuf buf = deserialized.retain();
				if (buf.readableBytes() != getSerializedBinaryLength()) {
					throw new IllegalArgumentException(
							"Fixed serializer with " + getSerializedBinaryLength() + " bytes has tried to serialize an element with "
									+ buf.readableBytes() + " bytes instead");
				}
				return buf;
			}

			@Override
			public int getSerializedBinaryLength() {
				return length;
			}
		};
	}

	static SerializerFixedBinaryLength<String, ByteBuf> utf8(ByteBufAllocator allocator, int length) {
		return new SerializerFixedBinaryLength<>() {
			@Override
			public @NotNull String deserialize(@NotNull ByteBuf serialized) throws SerializationException {
				try {
					if (serialized.readableBytes() != getSerializedBinaryLength()) {
						throw new SerializationException(
								"Fixed serializer with " + getSerializedBinaryLength() + " bytes has tried to deserialize an element with "
										+ serialized.readableBytes() + " bytes instead");
					}
					var result = serialized.toString(StandardCharsets.UTF_8);
					serialized.readerIndex(serialized.writerIndex());
					return result;
				} finally {
					serialized.release();
				}
			}

			@Override
			public @NotNull ByteBuf serialize(@NotNull String deserialized) throws SerializationException {
				// UTF-8 uses max. 3 bytes per char, so calculate the worst case.
				ByteBuf buf = allocator.buffer(ByteBufUtil.utf8MaxBytes(deserialized));
				try {
					ByteBufUtil.writeUtf8(buf, deserialized);
					if (buf.readableBytes() != getSerializedBinaryLength()) {
						throw new SerializationException("Fixed serializer with " + getSerializedBinaryLength()
								+ " bytes has tried to serialize an element with "
								+ buf.readableBytes() + " bytes instead");
					}
					return buf.retain();
				} finally {
					buf.release();
				}
			}

			@Override
			public int getSerializedBinaryLength() {
				return length;
			}
		};
	}

	static SerializerFixedBinaryLength<Integer, ByteBuf> intSerializer(ByteBufAllocator allocator) {
		return new SerializerFixedBinaryLength<>() {
			@Override
			public @NotNull Integer deserialize(@NotNull ByteBuf serialized) {
				try {
					if (serialized.readableBytes() != getSerializedBinaryLength()) {
						throw new IllegalArgumentException(
								"Fixed serializer with " + getSerializedBinaryLength() + " bytes has tried to deserialize an element with "
										+ serialized.readableBytes() + " bytes instead");
					}
					return serialized.readInt();
				} finally {
					serialized.release();
				}
			}

			@Override
			public @NotNull ByteBuf serialize(@NotNull Integer deserialized) {
				ByteBuf buf = allocator.buffer(Integer.BYTES);
				return buf.writeInt(deserialized);
			}

			@Override
			public int getSerializedBinaryLength() {
				return Integer.BYTES;
			}
		};
	}

	static SerializerFixedBinaryLength<Long, ByteBuf> longSerializer(ByteBufAllocator allocator) {
		return new SerializerFixedBinaryLength<>() {
			@Override
			public @NotNull Long deserialize(@NotNull ByteBuf serialized) {
				try {
					if (serialized.readableBytes() != getSerializedBinaryLength()) {
						throw new IllegalArgumentException(
								"Fixed serializer with " + getSerializedBinaryLength() + " bytes has tried to deserialize an element with "
										+ serialized.readableBytes() + " bytes instead");
					}
					return serialized.readLong();
				} finally {
					serialized.release();
				}
			}

			@Override
			public @NotNull ByteBuf serialize(@NotNull Long deserialized) {
				ByteBuf buf = allocator.buffer(Long.BYTES);
				return buf.writeLong(deserialized);
			}

			@Override
			public int getSerializedBinaryLength() {
				return Long.BYTES;
			}
		};
	}
}
