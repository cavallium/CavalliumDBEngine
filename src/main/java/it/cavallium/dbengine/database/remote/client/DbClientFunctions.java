package it.cavallium.dbengine.database.remote.client;

import io.grpc.ManagedChannel;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import it.cavallium.dbengine.proto.CavalliumDBEngineServiceGrpc;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import javax.net.ssl.SSLException;

public class DbClientFunctions extends CavalliumDBEngineServiceGrpc.CavalliumDBEngineServiceImplBase {

	private static final Logger logger = Logger.getLogger(DbClientFunctions.class.getName());
	private static final boolean SSL = false;

	private final ManagedChannel channel;
	private final CavalliumDBEngineServiceGrpc.CavalliumDBEngineServiceBlockingStub blockingStub;
	private final CavalliumDBEngineServiceGrpc.CavalliumDBEngineServiceStub stub;

	public CavalliumDBEngineServiceGrpc.CavalliumDBEngineServiceBlockingStub getBlockingStub() {
		return blockingStub;
	}

	public CavalliumDBEngineServiceGrpc.CavalliumDBEngineServiceStub getStub() {
		return stub;
	}

	public static SslContext buildSslContext(Path trustCertCollectionFilePath,
			Path clientCertChainFilePath,
			Path clientPrivateKeyFilePath) throws SSLException {
		SslContextBuilder builder = GrpcSslContexts.forClient();
		if (trustCertCollectionFilePath != null) {
			builder.trustManager(trustCertCollectionFilePath.toFile());
		}
		if (clientCertChainFilePath != null && clientPrivateKeyFilePath != null) {
			builder.keyManager(clientCertChainFilePath.toFile(), clientPrivateKeyFilePath.toFile());
		}
		return builder.build();
	}

	/**
	 * Construct client connecting to HelloWorld server at {@code host:port}.
	 */
	public DbClientFunctions(String host,
			int port,
			SslContext sslContext) throws SSLException {

		this(generateThis(host, port, sslContext));
	}

	private static ManagedChannel generateThis(String host, int port, SslContext sslContext) {
		var builder = NettyChannelBuilder.forAddress(host, port);
		if (SSL) {
			builder.sslContext(sslContext);
		} else {
			builder.usePlaintext();
		}
		return builder.build();
	}

	/**
	 * Construct client for accessing RouteGuide server using the existing channel.
	 */
	DbClientFunctions(ManagedChannel channel) {
		this.channel = channel;
		blockingStub = CavalliumDBEngineServiceGrpc.newBlockingStub(channel);
		stub = CavalliumDBEngineServiceGrpc.newStub(channel);
	}

	public void shutdown() throws InterruptedException {
		channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
	}
}
