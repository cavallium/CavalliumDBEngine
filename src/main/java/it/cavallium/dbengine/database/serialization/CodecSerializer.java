package it.cavallium.dbengine.database.serialization;

import io.net5.buffer.api.Buffer;
import io.net5.buffer.api.BufferAllocator;
import io.net5.buffer.api.Send;
import java.io.IOError;
import java.io.IOException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.warp.commonutils.error.IndexOutOfBoundsException;

public class CodecSerializer<A> implements Serializer<A> {

	private final BufferAllocator allocator;
	private final Codecs<A> deserializationCodecs;
	private final Codec<A> serializationCodec;
	private final int serializationCodecId;
	private final boolean microCodecs;

	/**
	 *
	 * @param microCodecs if true, allow only codecs with a value from 0 to 255 to save disk space
	 */
	public CodecSerializer(
			BufferAllocator allocator,
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
	public @NotNull DeserializationResult<A> deserialize(@Nullable Send<Buffer> serializedToReceive) {
		try (var is = new BufferDataInput(serializedToReceive)) {
			int codecId;
			if (microCodecs) {
				codecId = is.readUnsignedByte();
			} else {
				codecId = is.readInt();
			}
			var serializer = deserializationCodecs.getCodec(codecId);
			return new DeserializationResult<>(serializer.deserialize(is), is.getReadBytesCount());
		} catch (IOException ex) {
			// This shouldn't happen
			throw new IOError(ex);
		}
	}

	@Override
	public @NotNull Send<Buffer> serialize(@NotNull A deserialized) {
		try (Buffer buf = allocator.allocate(64)) {
			var os = new BufferDataOutput(buf);
			if (microCodecs) {
				os.writeByte(serializationCodecId);
			} else {
				os.writeInt(serializationCodecId);
			}
			serializationCodec.serialize(os, deserialized);
			return buf.send();
		} catch (IOException ex) {
			// This shouldn't happen
			throw new IOError(ex);
		}
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
