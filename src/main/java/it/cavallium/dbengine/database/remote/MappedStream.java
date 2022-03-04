package it.cavallium.dbengine.database.remote;

import io.netty.handler.codec.ByteToMessageCodec;
import java.util.function.Supplier;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks.Empty;
import reactor.netty.NettyOutbound;

public class MappedStream <SEND, RECV> implements AutoCloseable {

	private final Flux<RECV> inConn;
	private final NettyOutbound outConn;
	private final Supplier<ByteToMessageCodec<? super SEND>> outCodec;
	private final Empty<Void> streamTerminator;

	public MappedStream(NettyOutbound outConn, Supplier<ByteToMessageCodec<? super SEND>> outCodec, Flux<RECV> inConn, Empty<Void> streamTerminator) {
		this.inConn = inConn;
		this.outConn = outConn;
		this.outCodec = outCodec;
		this.streamTerminator = streamTerminator;
	}

	private NettyOutbound getOut() {
		return outConn.withConnection(conn -> conn.addHandler(outCodec.get()));
	}

	public Mono<Void> send(SEND item) {
		return getOut().sendObject(item).then();
	}

	public Mono<Void> sendMany(Flux<SEND> items) {
		return getOut().sendObject(items).then();
	}

	public Mono<RECV> receive() {
		return inConn.take(1, true).singleOrEmpty();
	}

	public Flux<RECV> receiveMany() {
		return inConn.hide();
	}

	@Override
	public void close() {
		streamTerminator.tryEmitEmpty();
	}
}
