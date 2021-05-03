package it.cavallium.dbengine.database.serialization;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import java.io.IOError;
import java.io.IOException;
import org.jetbrains.annotations.NotNull;
import org.warp.commonutils.error.IndexOutOfBoundsException;

public class CodecSerializer<A> implements Serializer<A, ByteBuf> {

	private final ByteBufAllocator allocator;
	private final Codecs<A> deserializationCodecs;
	private final Codec<A> serializationCodec;
	private final int serializationCodecId;
	private final boolean microCodecs;

	/**
	 *
	 * @param microCodecs if true, allow only codecs with a value from 0 to 255 to save disk space
	 */
	public CodecSerializer(
			ByteBufAllocator allocator,
			Codecs<A> deserializationCodecs,
			Codec<A> serializationCodec,
			int serializationCodecId,
			boolean microCodecs) {
		this.allocator = allocator;
		this.deserializationCodecs = deserializationCodecs;
		this.serializationCodec = serializationCodec;
		this.serializationCodecId = serializationCodecId;
		this.microCodecs = microCodecs;
		if (microCodecs && (serializationCodecId > 255 || serializationCodecId < 0)) {
			throw new IndexOutOfBoundsException(serializationCodecId, 0, 255);
		}
	}

	@Override
	public @NotNull A deserialize(@NotNull ByteBuf serialized) {
		try (var is = new ByteBufInputStream(serialized)) {
			int codecId;
			if (microCodecs) {
				codecId = is.readUnsignedByte();
			} else {
				codecId = is.readInt();
			}
			var serializer = deserializationCodecs.getCodec(codecId);
			return serializer.deserialize(is);
		} catch (IOException ex) {
			// This shouldn't happen
			throw new IOError(ex);
		} finally {
			serialized.release();
		}
	}

	@Override
	public @NotNull ByteBuf serialize(@NotNull A deserialized) {
		ByteBuf buf = allocator.buffer();
		try (var os = new ByteBufOutputStream(buf)) {
			if (microCodecs) {
				os.writeByte(serializationCodecId);
			} else {
				os.writeInt(serializationCodecId);
			}
			serializationCodec.serialize(os, deserialized);
		} catch (IOException ex) {
			// This shouldn't happen
			throw new IOError(ex);
		}
		return buf;
	}

	@SuppressWarnings("unused")
	public int getCodecHeadersBytes() {
		if (microCodecs) {
			return Byte.BYTES;
		} else {
			return Integer.BYTES;
		}
	}
}
