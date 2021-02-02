package it.cavallium.dbengine.database.serialization;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import org.jetbrains.annotations.NotNull;
import org.warp.commonutils.error.IndexOutOfBoundsException;

public class CodecSerializer<A> implements Serializer<A, byte[]> {

	private final Codecs<A> deserializationCodecs;
	private final Codec<A> serializationCodec;
	private final int serializationCodecId;
	private final boolean microCodecs;

	/**
	 *
	 * @param microCodecs if true, allow only codecs with a value from 0 to 255 to save disk space
	 */
	public CodecSerializer(Codecs<A> deserializationCodecs,
			Codec<A> serializationCodec,
			int serializationCodecId,
			boolean microCodecs) {
		this.deserializationCodecs = deserializationCodecs;
		this.serializationCodec = serializationCodec;
		this.serializationCodecId = serializationCodecId;
		this.microCodecs = microCodecs;
		if (microCodecs && (serializationCodecId > 255 || serializationCodecId < 0)) {
			throw new IndexOutOfBoundsException(serializationCodecId, 0, 255);
		}
	}

	@Override
	public @NotNull A deserialize(byte @NotNull [] serialized) {
		ByteBuf buf = Unpooled.wrappedBuffer(serialized);
		try (var is = new ByteBufInputStream(buf)) {
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
			throw new RuntimeException(ex);
		}
	}

	@Override
	public byte @NotNull [] serialize(@NotNull A deserialized) {
		ByteBuf buf = Unpooled.buffer(256);
		try (var os = new ByteBufOutputStream(buf)) {
			if (microCodecs) {
				os.writeByte(serializationCodecId);
			} else {
				os.writeInt(serializationCodecId);
			}
			serializationCodec.serialize(os, deserialized);
			os.flush();
			var bytes = new byte[buf.readableBytes()];
			buf.readBytes(bytes);
			return bytes;
		} catch (IOException ex) {
			// This shouldn't happen
			throw new RuntimeException(ex);
		}
	}
}
