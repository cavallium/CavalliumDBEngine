package it.cavallium.dbengine.database.remote;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageCodec;
import it.cavallium.dbengine.rpc.current.data.BoxedClientBoundRequest;
import it.cavallium.dbengine.rpc.current.data.BoxedClientBoundResponse;
import it.cavallium.dbengine.rpc.current.data.BoxedServerBoundRequest;
import it.cavallium.dbengine.rpc.current.data.BoxedServerBoundResponse;
import it.cavallium.dbengine.rpc.current.data.ClientBoundRequest;
import it.cavallium.dbengine.rpc.current.data.ClientBoundResponse;
import it.cavallium.dbengine.rpc.current.data.IBasicType;
import it.cavallium.dbengine.rpc.current.data.ServerBoundRequest;
import it.cavallium.dbengine.rpc.current.data.ServerBoundResponse;
import it.cavallium.dbengine.rpc.current.serializers.BoxedClientBoundRequestSerializer;
import it.cavallium.dbengine.rpc.current.serializers.BoxedClientBoundResponseSerializer;
import it.cavallium.dbengine.rpc.current.serializers.BoxedServerBoundRequestSerializer;
import it.cavallium.dbengine.rpc.current.serializers.BoxedServerBoundResponseSerializer;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.List;

public class RPCCodecs {

	public static class RPCClientBoundRequestDecoder extends ByteToMessageCodec<ClientBoundRequest> {

		public static final ChannelHandler INSTANCE = new RPCClientBoundRequestDecoder();

		@Override
		protected void encode(ChannelHandlerContext ctx, ClientBoundRequest msg, ByteBuf out) throws Exception {
			try (var bbos = new ByteBufOutputStream(out)) {
				try (var dos = new DataOutputStream(bbos)) {
					BoxedClientBoundRequestSerializer.INSTANCE.serialize(dos, BoxedClientBoundRequest.of(msg));
				}
			}
		}

		@Override
		protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {
			try (var bbis = new ByteBufInputStream(msg)) {
				try (var dis = new DataInputStream(bbis)) {
					out.add(BoxedClientBoundRequestSerializer.INSTANCE.deserialize(dis).val());
				}
			}
		}
	}

	public static class RPCServerBoundRequestDecoder extends ByteToMessageCodec<ServerBoundRequest> {

		public static final ChannelHandler INSTANCE = new RPCServerBoundRequestDecoder();

		@Override
		protected void encode(ChannelHandlerContext ctx, ServerBoundRequest msg, ByteBuf out) throws Exception {
			try (var bbos = new ByteBufOutputStream(out)) {
				try (var dos = new DataOutputStream(bbos)) {
					BoxedServerBoundRequestSerializer.INSTANCE.serialize(dos, BoxedServerBoundRequest.of(msg));
				}
			}
		}

		@Override
		protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {
			try (var bbis = new ByteBufInputStream(msg)) {
				try (var dis = new DataInputStream(bbis)) {
					out.add(BoxedServerBoundRequestSerializer.INSTANCE.deserialize(dis).val());
				}
			}
		}
	}

	public static class RPCClientBoundResponseDecoder extends ByteToMessageCodec<ClientBoundResponse> {

		public static final ChannelHandler INSTANCE = new RPCClientBoundResponseDecoder();

		@Override
		protected void encode(ChannelHandlerContext ctx, ClientBoundResponse msg, ByteBuf out) throws Exception {
			try (var bbos = new ByteBufOutputStream(out)) {
				try (var dos = new DataOutputStream(bbos)) {
					BoxedClientBoundResponseSerializer.INSTANCE.serialize(dos, BoxedClientBoundResponse.of(msg));
				}
			}
		}

		@Override
		protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {
			try (var bbis = new ByteBufInputStream(msg)) {
				try (var dis = new DataInputStream(bbis)) {
					out.add(BoxedClientBoundResponseSerializer.INSTANCE.deserialize(dis).val());
				}
			}
		}
	}

	public static class RPCServerBoundResponseDecoder extends ByteToMessageCodec<ServerBoundResponse> {

		public static final ChannelHandler INSTANCE = new RPCServerBoundResponseDecoder();

		@Override
		protected void encode(ChannelHandlerContext ctx, ServerBoundResponse msg, ByteBuf out) throws Exception {
			try (var bbos = new ByteBufOutputStream(out)) {
				try (var dos = new DataOutputStream(bbos)) {
					BoxedServerBoundResponseSerializer.INSTANCE.serialize(dos, BoxedServerBoundResponse.of(msg));
				}
			}
		}

		@Override
		protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {
			try (var bbis = new ByteBufInputStream(msg)) {
				try (var dis = new DataInputStream(bbis)) {
					out.add(BoxedServerBoundResponseSerializer.INSTANCE.deserialize(dis).val());
				}
			}
		}
	}

	public static class RPCClientAlternateDecoder extends ByteToMessageCodec<IBasicType> {

		public static final ChannelHandler INSTANCE = new RPCClientAlternateDecoder();

		private boolean alternated = false;

		@Override
		protected void encode(ChannelHandlerContext ctx, IBasicType msg, ByteBuf out) throws Exception {
			try (var bbos = new ByteBufOutputStream(out)) {
				try (var dos = new DataOutputStream(bbos)) {
					if (!alternated) {
						BoxedServerBoundRequestSerializer.INSTANCE.serialize(dos,
								BoxedServerBoundRequest.of((ServerBoundRequest) msg)
						);
					} else {
						BoxedClientBoundResponseSerializer.INSTANCE.serialize(dos,
								BoxedClientBoundResponse.of((ClientBoundResponse) msg)
						);
					}
					alternated = !alternated;
				}
			}
		}

		@Override
		protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {
			try (var bbis = new ByteBufInputStream(msg)) {
				try (var dis = new DataInputStream(bbis)) {
					if (!alternated) {
						out.add(BoxedServerBoundRequestSerializer.INSTANCE.deserialize(dis).val());
					} else {
						out.add(BoxedClientBoundResponseSerializer.INSTANCE.deserialize(dis).val());
					}
					alternated = !alternated;
				}
			}
		}
	}

	public static class RPCServerAlternateDecoder extends ByteToMessageCodec<IBasicType> {

		public static final ChannelHandler INSTANCE = new RPCServerAlternateDecoder();

		private boolean alternated = false;

		@Override
		protected void encode(ChannelHandlerContext ctx, IBasicType msg, ByteBuf out) throws Exception {
			try (var bbos = new ByteBufOutputStream(out)) {
				try (var dos = new DataOutputStream(bbos)) {
					if (!alternated) {
						BoxedClientBoundRequestSerializer.INSTANCE.serialize(dos,
								BoxedClientBoundRequest.of((ClientBoundRequest) msg)
						);
					} else {
						BoxedServerBoundResponseSerializer.INSTANCE.serialize(dos,
								BoxedServerBoundResponse.of((ServerBoundResponse) msg)
						);
					}
					alternated = !alternated;
				}
			}
		}

		@Override
		protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {
			try (var bbis = new ByteBufInputStream(msg)) {
				try (var dis = new DataInputStream(bbis)) {
					if (!alternated) {
						out.add(BoxedClientBoundRequestSerializer.INSTANCE.deserialize(dis).val());
					} else {
						out.add(BoxedServerBoundResponseSerializer.INSTANCE.deserialize(dis).val());
					}
					alternated = !alternated;
				}
			}
		}
	}
}
