package it.cavallium.dbengine.database.remote;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageCodec;
import it.cavallium.dbengine.rpc.current.data.BoxedRPCEvent;
import it.cavallium.dbengine.rpc.current.data.ClientBoundRequest;
import it.cavallium.dbengine.rpc.current.data.ClientBoundResponse;
import it.cavallium.dbengine.rpc.current.data.IBasicType;
import it.cavallium.dbengine.rpc.current.data.RPCEvent;
import it.cavallium.dbengine.rpc.current.data.ServerBoundRequest;
import it.cavallium.dbengine.rpc.current.data.ServerBoundResponse;
import it.cavallium.dbengine.rpc.current.serializers.BoxedRPCEventSerializer;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.List;

public class RPCCodecs {

	public static class RPCEventCodec extends ByteToMessageCodec<RPCEvent> {

		public static final ChannelHandler INSTANCE = new RPCEventCodec();

		@Override
		protected void encode(ChannelHandlerContext ctx, RPCEvent msg, ByteBuf out) throws Exception {
			try (var bbos = new ByteBufOutputStream(out)) {
				try (var dos = new DataOutputStream(bbos)) {
					BoxedRPCEventSerializer.INSTANCE.serialize(dos, BoxedRPCEvent.of(msg));
				}
			}
		}

		@Override
		protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {
			try (var bbis = new ByteBufInputStream(msg)) {
				try (var dis = new DataInputStream(bbis)) {
					out.add(BoxedRPCEventSerializer.INSTANCE.deserialize(dis).val());
				}
			}
		}
	}
}
