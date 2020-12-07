package it.cavallium.dbengine.database.remote.server;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.netty.GrpcSslContexts;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import java.io.IOException;
import java.nio.file.Path;
import it.cavallium.dbengine.database.disk.LLLocalDatabaseConnection;

public class DbServerManager {

	private Server server;

	public boolean stopped;

	private final LLLocalDatabaseConnection databaseConnection;
	private final String host;
	private final int port;
	private final Path certChainFilePath;
	private final Path privateKeyFilePath;
	private final Path trustCertCollectionFilePath;

	public DbServerManager(LLLocalDatabaseConnection databaseConnection, String host, int port,
			Path certChainFilePath, Path privateKeyFilePath, Path trustCertCollectionFilePath) {
		this.databaseConnection = databaseConnection;
		this.host = host;
		this.port = port;
		this.certChainFilePath = certChainFilePath;
		this.privateKeyFilePath = privateKeyFilePath;
		this.trustCertCollectionFilePath = trustCertCollectionFilePath;
	}

	private SslContextBuilder getSslContextBuilder() {
		SslContextBuilder sslClientContextBuilder = SslContextBuilder
				.forServer(certChainFilePath.toFile(),
						privateKeyFilePath.toFile());
		if (trustCertCollectionFilePath != null) {
			sslClientContextBuilder.trustManager(trustCertCollectionFilePath.toFile());
			sslClientContextBuilder.clientAuth(ClientAuth.REQUIRE);
		}
		return GrpcSslContexts.configure(sslClientContextBuilder,
				SslProvider.OPENSSL);
	}

	public void start() throws IOException {
		var srvBuilder = ServerBuilder.forPort(port)
				.addService(new DbServerFunctions(databaseConnection));
		server = srvBuilder.build()
				.start();
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			if (!stopped) {
				// Use stderr here since the logger may have been reset by its JVM shutdown hook.
				System.err.println("*** shutting down gRPC server since JVM is shutting down");
				this.stop();
				try {
					databaseConnection.disconnect();
				} catch (IOException e) {
					e.printStackTrace();
				}
				System.err.println("*** server shut down");
			}
		}));
		System.out.println("Server started, listening on " + port);
	}

	public void stop() {
		stopped = true;
		if (server != null) {
			try {
				server.shutdown();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
			try {
				blockUntilShutdown();
			} catch (InterruptedException ex) {
				ex.printStackTrace();
			}
		}
		System.out.println("Server stopped.");
	}

	/**
	 * Await termination on the main thread since the grpc library uses daemon threads.
	 */
	void blockUntilShutdown() throws InterruptedException {
		if (server != null) {
			server.awaitTermination();
		}
	}

}
