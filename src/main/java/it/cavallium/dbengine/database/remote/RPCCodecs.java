package it.cavallium.dbengine.database.remote;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageCodec;
import it.cavallium.dbengine.rpc.current.data.BoxedRPCEvent;
import it.cavallium.dbengine.rpc.current.data.RPCEvent;
import it.cavallium.dbengine.rpc.current.serializers.BoxedRPCEventSerializer;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.List;

public class RPCCodecs {

	public static class RPCEventCodec extends ByteToMessageCodec<RPCEvent> {

		public static final ChannelHandler INSTANCE = new RPCEventCodec();
		public static final BoxedRPCEventSerializer SERIALIZER_INSTANCE = new BoxedRPCEventSerializer();

		@Override
		protected void encode(ChannelHandlerContext ctx, RPCEvent msg, ByteBuf out) throws Exception {
			try (var bbos = new ByteBufOutputStream(out)) {
				try (var dos = new DataOutputStream(bbos)) {
					SERIALIZER_INSTANCE.serialize(dos, BoxedRPCEvent.of(msg));
				}
			}
		}

		@Override
		protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {
			try (var bbis = new ByteBufInputStream(msg)) {
				try (var dis = new DataInputStream(bbis)) {
					out.add(SERIALIZER_INSTANCE.deserialize(dis).val());
				}
			}
		}
	}
}
