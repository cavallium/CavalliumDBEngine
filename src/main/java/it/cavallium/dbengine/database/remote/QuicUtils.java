package it.cavallium.dbengine.database.remote;

import io.netty.handler.codec.ByteToMessageCodec;
import it.cavallium.data.generator.nativedata.NullableString;
import it.cavallium.dbengine.rpc.current.data.RPCCrash;
import it.cavallium.dbengine.rpc.current.data.RPCEvent;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.bytes.ByteList;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Level;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.Empty;
import reactor.core.publisher.Sinks.One;
import reactor.netty.NettyInbound;
import reactor.netty.NettyOutbound;
import reactor.netty.incubator.quic.QuicConnection;

public class QuicUtils {

	public static final Mono<?> NO_RESPONSE_ERROR = Mono.error(NoResponseReceivedException::new);

	public static byte[] toArrayNoCopy(ByteList b) {
		if (b instanceof ByteArrayList bal) {
			return bal.elements();
		} else {
			return b.toByteArray();
		}
	}

	public static String toString(ByteList b) {
		return new String(QuicUtils.toArrayNoCopy(b), StandardCharsets.UTF_8);
	}

	public record QuicStream(NettyInbound in, NettyOutbound out) {}

	public static Mono<RPCEvent> catchRPCErrors(Throwable error) {
		return Mono.just(new RPCCrash(500, NullableString.ofNullableBlank(error.getMessage())));
	}

	private static <SEND, RECV> RECV extractResponse(SEND request, RECV response) {
		return response;
	}

	/**
	 * Create a general purpose QUIC stream
	 */
	public static Mono<QuicStream> createStream(QuicConnection quicConnection, Mono<Void> streamTerminator) {
		return Mono.defer(() -> {
			One<QuicStream> inOutSink = Sinks.one();
			return quicConnection
					.createStream((in, out) -> Mono
							.fromRunnable(() -> inOutSink.tryEmitValue(new QuicStream(in, out)).orThrow())
							.then(streamTerminator))
					.then(inOutSink.asMono());
		});
	}

	/**
	 * Send a single request, receive a single response
	 */
	@SuppressWarnings("unchecked")
	public static <SEND, RECV> Mono<MappedStream<SEND, RECV>> createMappedStream(
			QuicConnection quicConnection,
			Supplier<ByteToMessageCodec<? super SEND>> sendCodec,
			Supplier<ByteToMessageCodec<? super RECV>> recvCodec) {
		return Mono.defer(() -> {
			Empty<Void> streamTerminator = Sinks.empty();
			return QuicUtils
					.createStream(quicConnection, streamTerminator.asMono())
					.map(stream -> {
						Flux<RECV> inConn = Flux.defer(() -> (Flux<RECV>) stream
										.in()
										.withConnection(conn -> conn.addHandler(recvCodec.get()))
										.receiveObject()
										.log("RECEIVE_OBJECT_FROM_SERVER", Level.FINEST))
								.publish(1)
								.refCount();
						return new MappedStream<>(stream.out, sendCodec, inConn, streamTerminator);
					})
					.single();
		});
	}

	/**
	 * Send a single request, receive a single response
	 */
	@SuppressWarnings("unchecked")
	public static <SEND, RECV> Mono<RECV> sendSimpleRequest(QuicConnection quicConnection,
			Supplier<ByteToMessageCodec<? super SEND>> sendCodec,
			Supplier<ByteToMessageCodec<? super RECV>> recvCodec,
			SEND req) {
		return QuicUtils
				.createMappedStream(quicConnection, sendCodec, recvCodec)
				.flatMap(stream -> {
					var recv = stream.receive().log("SR-Receive", Level.FINEST);
					var send = stream.send(req).log("SR-Send", Level.FINEST);
					return send
							.then(recv)
							.doFinally(s -> stream.close());
				})
				.switchIfEmpty((Mono<RECV>) NO_RESPONSE_ERROR)
				.log("SR-Result", Level.FINEST);
	}

	/**
	 * Send n requests, receive n responses
	 */
	public static <SEND, RECV> Flux<RECV> sendSimpleRequestFlux(QuicConnection quicConnection,
			Supplier<ByteToMessageCodec<? super SEND>> sendCodec,
			Supplier<ByteToMessageCodec<? super RECV>> recvCodec,
			Publisher<SEND> requestFlux) {
		return QuicUtils
				.createMappedStream(quicConnection, sendCodec, recvCodec)
				.flatMapMany(stream -> {
					var sends = Flux
							.from(requestFlux)
							.log("SR-Send", Level.FINEST)
							.concatMap(request -> stream.send(request)
									.thenReturn(request));
					var receives = stream
							.receiveMany()
							.log("SR-Receive", Level.FINEST);
					return Flux
							.zip(sends, receives, QuicUtils::extractResponse)
							.doFinally(s -> stream.close());
				})
				.log("SR-Result", Level.FINEST);
	}

	/**
	 * Send update
	 */
	public static <T> Mono<T> sendUpdate(QuicConnection quicConnection,
			Supplier<ByteToMessageCodec<? super T>> codec,
			T request,
			Function<T, Mono<T>> updater) {
		return QuicUtils
				.createMappedStream(quicConnection, codec, codec)
				.flatMapMany(stream -> {
					//noinspection unchecked
					var firstRequest = (Mono<T>) stream
							.send(request)
							.then();
					var receives = stream
							.receiveMany();
					One<T> firstResponseSink = Sinks.one();
					//noinspection unchecked
					var firstResponse = (Mono<T>) receives
							.elementAt(0)
							.switchIfEmpty((Mono<? extends T>) NO_RESPONSE_ERROR)
							.mapNotNull(value -> {
								if (value instanceof RPCCrash crash) {
									firstResponseSink.tryEmitEmpty();
									//noinspection unchecked
									return (T) crash;
								} else {
									firstResponseSink.tryEmitValue(value);
									return null;
								}
							})
							.doOnCancel(firstResponseSink::tryEmitEmpty);
					//noinspection unchecked
					var secondResponse = Mono
							// FirstResponse returns only if it's RPCCrash.
							// firstWithValue returns the crash first if it happens, otherwise it will
							// return receives
							.firstWithValue(
									firstResponse,
									receives.elementAt(1)
							)
							.switchIfEmpty((Mono<? extends T>) NO_RESPONSE_ERROR);
					//noinspection unchecked
					var secondRequest = (Mono<T>) firstResponseSink
							.asMono()
							.flatMap(updater)
							.flatMap(stream::send);
					return Flux
							.merge(firstRequest, firstResponse.then(Mono.empty()), secondRequest, secondResponse)
							.doFinally(s -> stream.close());
				})
				.singleOrEmpty();
	}
}
