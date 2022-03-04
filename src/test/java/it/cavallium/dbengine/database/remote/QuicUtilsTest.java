package it.cavallium.dbengine.database.remote;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.incubator.codec.quic.InsecureQuicTokenHandler;
import io.netty.incubator.codec.quic.QuicConnectionIdGenerator;
import io.netty.incubator.codec.quic.QuicSslContext;
import io.netty.incubator.codec.quic.QuicSslContextBuilder;
import it.cavallium.data.generator.nativedata.NullableString;
import it.cavallium.dbengine.database.remote.RPCCodecs.RPCEventCodec;
import it.cavallium.dbengine.rpc.current.data.Empty;
import it.cavallium.dbengine.rpc.current.data.RPCCrash;
import it.cavallium.dbengine.rpc.current.data.RPCEvent;
import it.cavallium.dbengine.rpc.current.data.SingletonGet;
import it.cavallium.dbengine.rpc.current.data.nullables.NullableLLSnapshot;
import java.net.InetSocketAddress;
import java.security.cert.CertificateException;
import java.time.Duration;
import java.util.List;
import java.util.logging.Level;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.netty.Connection;
import reactor.netty.incubator.quic.QuicClient;
import reactor.netty.incubator.quic.QuicConnection;

class QuicUtilsTest {

	private static final int NORMAL = 0;
	private static final int WAIT_TIME = 1;
	private static final int FAIL_IMMEDIATELY = 2;
	private static final int WAIT_TIME_THEN_FAIL = 3;


	private Connection serverConn;
	private QuicConnection clientConn;
	private InetSocketAddress clientAddress;
	private InetSocketAddress serverAddress;


	@BeforeEach
	void setUp() throws CertificateException {
		var selfSignedCert = new SelfSignedCertificate();
		this.clientAddress = new InetSocketAddress("localhost", 8081);
		this.serverAddress = new InetSocketAddress("localhost", 8080);
		QuicSslContext sslContext = QuicSslContextBuilder
				.forServer(selfSignedCert.key(), null, selfSignedCert.cert())
				.applicationProtocols("db/0.9")
				.clientAuth(ClientAuth.NONE)
				.build();
		var qs = reactor.netty.incubator.quic.QuicServer
				.create()
				.tokenHandler(InsecureQuicTokenHandler.INSTANCE)
				.bindAddress(() -> serverAddress)
				.secure(sslContext)
				.idleTimeout(Duration.ofSeconds(30))
				.connectionIdAddressGenerator(QuicConnectionIdGenerator.randomGenerator())
				.initialSettings(spec -> spec
						.maxData(10000000)
						.maxStreamDataBidirectionalLocal(1000000)
						.maxStreamDataBidirectionalRemote(1000000)
						.maxStreamsBidirectional(100)
						.maxStreamsUnidirectional(100)
				)
				.handleStream((in, out) -> in
						.withConnection(conn -> conn.addHandler(new RPCEventCodec()))
						.receiveObject()
						.cast(RPCEvent.class)
						.log("recv", Level.FINEST)
						.flatMapSequential(req -> (switch ((int) ((SingletonGet) req).singletonId()) {
							case NORMAL -> Mono.<RPCEvent>just(Empty.of());
							case FAIL_IMMEDIATELY -> Mono.<RPCEvent>error(new Throwable("Expected error"));
							case WAIT_TIME -> Mono.delay(Duration.ofSeconds(3)).<RPCEvent>thenReturn(Empty.of());
							case WAIT_TIME_THEN_FAIL -> Mono
									.delay(Duration.ofSeconds(3))
									.then(Mono.<RPCEvent>error(new Throwable("Expected error")));
							default -> Mono.<RPCEvent>error(new UnsupportedOperationException("Unsupported request id " + req));
						}).log("Server", Level.SEVERE, SignalType.ON_ERROR).onErrorResume(QuicUtils::catchRPCErrors))
						.concatMap(message -> Mono.defer(() -> out
								.withConnection(conn -> conn.addHandler(new RPCEventCodec()))
								.sendObject(message)
								.then())
								.log("send", Level.FINEST)
						)
				);
		this.serverConn = qs.bindNow();

		var clientSslContext = QuicSslContextBuilder
				.forClient()
				.trustManager(InsecureTrustManagerFactory.INSTANCE)
				.applicationProtocols("db/0.9")
				.build();
		this.clientConn = QuicClient.create()
				.bindAddress(() -> new InetSocketAddress(0))
				.remoteAddress(() -> serverAddress)
				.secure(clientSslContext)
				.idleTimeout(Duration.ofSeconds(30))
				.initialSettings(spec -> spec
						.maxData(10000000)
						.maxStreamDataBidirectionalLocal(1000000)
				)
				.connectNow();
	}

	@AfterEach
	void tearDown() {
		if (clientConn != null) {
			clientConn.disposeNow();
		}
		if (serverConn != null) {
			serverConn.disposeNow();
		}
	}

	@Test
	void sendSimpleRequest() {
		RPCEvent response = QuicUtils.<RPCEvent, RPCEvent>sendSimpleRequest(clientConn,
				RPCEventCodec::new,
				RPCEventCodec::new,
				new SingletonGet(NORMAL, NullableLLSnapshot.empty())
		).blockOptional().orElseThrow();
		assertEquals(Empty.of(), response);
	}

	@Test
	void sendSimpleRequestFlux() {
		List<RPCEvent> results = QuicUtils.<RPCEvent, RPCEvent>sendSimpleRequestFlux(clientConn,
				RPCEventCodec::new,
				RPCEventCodec::new,
				Flux.just(
						new SingletonGet(NORMAL, NullableLLSnapshot.empty()),
						new SingletonGet(NORMAL, NullableLLSnapshot.empty()),
						new SingletonGet(NORMAL, NullableLLSnapshot.empty()),
						new SingletonGet(NORMAL, NullableLLSnapshot.empty()),
						new SingletonGet(NORMAL, NullableLLSnapshot.empty())
				)
		).collectList().blockOptional().orElseThrow();
		assertEquals(5, results.size());
		assertEquals(List.of(Empty.of(), Empty.of(), Empty.of(), Empty.of(), Empty.of()), results);
	}

	@Test
	void sendUpdateFluxNormal() {
		RPCEvent results = QuicUtils.<RPCEvent>sendUpdate(clientConn,
				RPCEventCodec::new,
				new SingletonGet(NORMAL, NullableLLSnapshot.empty()),
				serverData -> Mono.fromCallable(() -> {
					assertEquals(Empty.of(), serverData);
					return new SingletonGet(NORMAL, NullableLLSnapshot.empty());
				})
		).blockOptional().orElseThrow();
		assertEquals(Empty.of(), results);
	}

	@Test
	void sendUpdateFluxSlowClient() {
		RPCEvent results = QuicUtils.<RPCEvent>sendUpdate(clientConn,
				RPCEventCodec::new,
				new SingletonGet(NORMAL, NullableLLSnapshot.empty()),
				serverData -> Mono.<RPCEvent>fromCallable(() -> {
					assertEquals(Empty.of(), serverData);
					return new SingletonGet(NORMAL, NullableLLSnapshot.empty());
				}).delayElement(Duration.ofSeconds(2))
		).blockOptional().orElseThrow();
		assertEquals(Empty.of(), results);
	}

	@Test
	void sendUpdateFluxSlowServer() {
		RPCEvent results = QuicUtils.<RPCEvent>sendUpdate(clientConn,
				RPCEventCodec::new,
				new SingletonGet(WAIT_TIME, NullableLLSnapshot.empty()),
				serverData -> Mono.fromCallable(() -> {
					assertEquals(Empty.of(), serverData);
					return new SingletonGet(WAIT_TIME, NullableLLSnapshot.empty());
				})
		).blockOptional().orElseThrow();
		assertEquals(Empty.of(), results);
	}

	@Test
	void sendUpdateFluxSlowClientAndServer() {
		RPCEvent results = QuicUtils.<RPCEvent>sendUpdate(clientConn,
				RPCEventCodec::new,
				new SingletonGet(WAIT_TIME, NullableLLSnapshot.empty()),
				serverData -> Mono.<RPCEvent>fromCallable(() -> {
					assertEquals(Empty.of(), serverData);
					return new SingletonGet(WAIT_TIME, NullableLLSnapshot.empty());
				}).delayElement(Duration.ofSeconds(2))
		).blockOptional().orElseThrow();
		assertEquals(Empty.of(), results);
	}

	@Test
	void sendUpdateClientFail() {
		class ExpectedException extends Throwable {}

		assertThrows(ExpectedException.class, () -> {
			try {
				RPCEvent results = QuicUtils
						.<RPCEvent>sendUpdate(clientConn,
								RPCEventCodec::new,
								new SingletonGet(NORMAL, NullableLLSnapshot.empty()),
								serverData -> Mono.error(new ExpectedException())
						)
						.blockOptional()
						.orElseThrow();
			} catch (Throwable e) {
				throw e.getCause();
			}
		});
	}

	@Test
	void sendUpdateServerFail1() {
		RPCEvent results = QuicUtils.<RPCEvent>sendUpdate(clientConn,
				RPCEventCodec::new,
				new SingletonGet(FAIL_IMMEDIATELY, NullableLLSnapshot.empty()),
				serverData -> Mono.fromCallable(() -> {
					fail("Called update");
					return new SingletonGet(NORMAL, NullableLLSnapshot.empty());
				})
		).blockOptional().orElseThrow();
		assertEquals(RPCCrash.of(500, NullableString.of("Expected error")), results);
	}

	@Test
	void sendUpdateServerFail2() {
		RPCEvent results = QuicUtils.<RPCEvent>sendUpdate(clientConn,
				RPCEventCodec::new,
				new SingletonGet(NORMAL, NullableLLSnapshot.empty()),
				serverData -> Mono.fromCallable(() -> {
					assertEquals(Empty.of(), serverData);
					return new SingletonGet(FAIL_IMMEDIATELY, NullableLLSnapshot.empty());
				})
		).blockOptional().orElseThrow();
		assertEquals(RPCCrash.of(500, NullableString.of("Expected error")), results);
	}

	@Test
	void sendSimpleRequestConcurrently() {
		// Send the request a second time
		var requestMono = QuicUtils.<RPCEvent, RPCEvent>sendSimpleRequest(clientConn,
				RPCEventCodec::new,
				RPCEventCodec::new,
				new SingletonGet(NORMAL, NullableLLSnapshot.empty())
		);
		var results = Flux
				.merge(requestMono, requestMono, requestMono, requestMono, requestMono)
				.collectList()
				.blockOptional()
				.orElseThrow();
		assertEquals(5, results.size());
		assertEquals(List.of(Empty.of(), Empty.of(), Empty.of(), Empty.of(), Empty.of()), results);
	}

	@Test
	void sendFailedRequest() {
		RPCEvent response = QuicUtils.<RPCEvent, RPCEvent>sendSimpleRequest(clientConn,
				RPCEventCodec::new,
				RPCEventCodec::new,
				new SingletonGet(FAIL_IMMEDIATELY, NullableLLSnapshot.empty())
		).blockOptional().orElseThrow();
		assertEquals(RPCCrash.of(500, NullableString.of("Expected error")), response);
	}

	@Test
	void createStream() {
	}
}