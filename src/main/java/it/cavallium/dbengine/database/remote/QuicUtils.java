package it.cavallium.dbengine.database.remote;

import io.netty.handler.codec.ByteToMessageCodec;
import io.netty5.buffer.api.Buffer;
import io.netty5.buffer.api.Send;
import it.cavallium.data.generator.nativedata.NullableString;
import it.cavallium.dbengine.rpc.current.data.RPCCrash;
import it.cavallium.dbengine.rpc.current.data.RPCEvent;
import it.cavallium.dbengine.rpc.current.data.nullables.NullableBytes;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.bytes.ByteList;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Level;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
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

	@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
	public static NullableBytes toBytes(Optional<Send<Buffer>> valueSendOpt) {
		if (valueSendOpt.isPresent()) {
			try (var value = valueSendOpt.get().receive()) {
				var bytes = new byte[value.readableBytes()];
				value.copyInto(value.readerOffset(), bytes, 0, bytes.length);
				return NullableBytes.ofNullable(ByteList.of(bytes));
			}
		} else {
			return NullableBytes.empty();
		}
	}

	public static Mono<NullableBytes> toBytes(Mono<Send<Buffer>> valueSendOptMono) {
		return valueSendOptMono.map(valueSendOpt -> {
			try (var value = valueSendOpt.receive()) {
				var bytes = new byte[value.readableBytes()];
				value.copyInto(value.readerOffset(), bytes, 0, bytes.length);
				return NullableBytes.ofNullable(ByteList.of(bytes));
			}
		}).defaultIfEmpty(NullableBytes.empty());
	}

	public record QuicStream(NettyInbound in, NettyOutbound out) {}

	public static Mono<RPCEvent> catchRPCErrors(@NotNull Throwable error) {
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
			@NotNull QuicConnection quicConnection,
			@NotNull Supplier<ByteToMessageCodec<? super SEND>> sendCodec,
			@Nullable Supplier<ByteToMessageCodec<? super RECV>> recvCodec) {
		return Mono.defer(() -> {
			Empty<Void> streamTerminator = Sinks.empty();
			return QuicUtils
					.createStream(quicConnection, streamTerminator.asMono())
					.map(stream -> {
						Flux<RECV> inConn;
						if (recvCodec == null) {
							inConn = Flux.error(() -> new UnsupportedOperationException("Receiving responses is supported"));
						} else {
							inConn = Flux.defer(() -> (Flux<RECV>) stream
											.in()
											.withConnection(conn -> conn.addHandler(recvCodec.get()))
											.receiveObject()
											.log("ClientBoundEvent", Level.FINEST)
									)
									.publish(1)
									.refCount();
						}
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
					var recv = stream.receive().log("ClientBoundEvent", Level.FINEST);
					var send = stream.send(req).log("ServerBoundEvent", Level.FINEST);
					return send
							.then(recv)
							.doFinally(s -> stream.close());
				})
				.map(QuicUtils::mapErrors)
				.switchIfEmpty((Mono<RECV>) NO_RESPONSE_ERROR);
	}

	/**
	 * Send a single request, receive a single response
	 */

	public static <SEND> Mono<Void> sendSimpleEvent(QuicConnection quicConnection,
			Supplier<ByteToMessageCodec<? super SEND>> sendCodec,
			SEND req) {
		return QuicUtils
				.createMappedStream(quicConnection, sendCodec, null)
				.flatMap(stream -> {
					var send = stream.send(req).log("ServerBoundEvent", Level.FINEST);
					return send.doFinally(s -> stream.close());
				})
				.map(QuicUtils::mapErrors)
				.then();
	}

	private static <R> R mapErrors(R value) {
		if (value instanceof RPCCrash crash) {
			throw new RPCException(crash.code(), crash.message().orElse(null));
		} else {
			return value;
		}
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
							.log("ServerBoundEvent", Level.FINEST)
							.concatMap(request -> stream.send(request)
									.thenReturn(request));
					var receives = stream
							.receiveMany()
							.log("ClientBoundEvent", Level.FINEST);
					return Flux
							.zip(sends, receives, QuicUtils::extractResponse)
							.doFinally(s -> stream.close());
				})
				.map(QuicUtils::mapErrors)
				.log("ServerBoundEvent", Level.FINEST);
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
				.map(QuicUtils::mapErrors)
				.singleOrEmpty();
	}
}
