package it.cavallium.dbengine.database.serialization;

import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.BufferAllocator;
import io.netty5.buffer.api.Send;
import java.io.IOError;
import java.io.IOException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.warp.commonutils.error.IndexOutOfBoundsException;

public class CodecSerializer<A> implements Serializer<A> {

	private final Codecs<A> deserializationCodecs;
	private final Codec<A> serializationCodec;
	private final int serializationCodecId;
	private final boolean microCodecs;
	private final int serializedSizeHint;

	/**
	 *
	 * @param microCodecs if true, allow only codecs with a value from 0 to 255 to save disk space
	 * @param serializedSizeHint suggested default buffer size, -1 if unknown
	 */
	public CodecSerializer(
			Codecs<A> deserializationCodecs,
			Codec<A> serializationCodec,
			int serializationCodecId,
			boolean microCodecs,
			int serializedSizeHint) {
		this.deserializationCodecs = deserializationCodecs;
		this.serializationCodec = serializationCodec;
		this.serializationCodecId = serializationCodecId;
		this.microCodecs = microCodecs;
		if (microCodecs && (serializationCodecId > 255 || serializationCodecId < 0)) {
			throw new IndexOutOfBoundsException(serializationCodecId, 0, 255);
		}
		if (serializedSizeHint != -1) {
			this.serializedSizeHint = (microCodecs ? Byte.BYTES : Integer.BYTES) + serializedSizeHint;
		} else {
			this.serializedSizeHint = -1;
		}
	}

	@Override
	public int getSerializedSizeHint() {
		return serializedSizeHint;
	}

	@Override
	public @NotNull A deserialize(@NotNull Buffer serializedBuf) throws SerializationException {
		try {
			var is = new BufferDataInputShared(serializedBuf);
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
		}
	}

	@Override
	public void serialize(@NotNull A deserialized, Buffer output) throws SerializationException {
		try {
			var os = new BufferDataOutput(output);
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
