package it.cavallium.dbengine.database.remote.client;

import com.google.protobuf.ByteString;
import io.grpc.StatusRuntimeException;
import it.cavallium.dbengine.proto.CavalliumDBEngineServiceGrpc;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import javax.net.ssl.SSLException;
import it.cavallium.dbengine.database.Column;
import it.cavallium.dbengine.database.LLDatabaseConnection;
import it.cavallium.dbengine.database.LLKeyValueDatabase;
import it.cavallium.dbengine.database.LLLuceneIndex;
import it.cavallium.dbengine.database.analyzer.TextFieldsAnalyzer;
import it.cavallium.dbengine.proto.DatabaseOpenRequest;
import it.cavallium.dbengine.proto.Empty;
import it.cavallium.dbengine.proto.LuceneIndexOpenRequest;
import it.cavallium.dbengine.proto.ResetConnectionRequest;

public class LLRemoteDatabaseConnection implements LLDatabaseConnection {

	private final String address;
	private final int port;
	private final Path trustCertCollectionFilePath;
	private final Path clientCertChainFilePath;
	private final Path clientPrivateKeyFilePath;
	private DbClientFunctions client;
	private CavalliumDBEngineServiceGrpc.CavalliumDBEngineServiceBlockingStub blockingStub;

	public LLRemoteDatabaseConnection(String address, int port, Path trustCertCollectionFilePath,
			Path clientCertChainFilePath,
			Path clientPrivateKeyFilePath) {
		this.address = address;
		this.port = port;
		this.trustCertCollectionFilePath = trustCertCollectionFilePath;
		this.clientCertChainFilePath = clientCertChainFilePath;
		this.clientPrivateKeyFilePath = clientPrivateKeyFilePath;
	}

	@Override
	public void connect() throws IOException {
		try {
			this.client = new DbClientFunctions(address, port,
					DbClientFunctions.buildSslContext(trustCertCollectionFilePath, clientCertChainFilePath,
							clientPrivateKeyFilePath));
			this.blockingStub = client.getBlockingStub();
			//noinspection ResultOfMethodCallIgnored
			blockingStub.resetConnection(ResetConnectionRequest.newBuilder().build());
		} catch (SSLException | StatusRuntimeException e) {
			throw new IOException(e);
		}
	}

	@Override
	public LLKeyValueDatabase getDatabase(String name, List<Column> columns, boolean lowMemory) throws IOException {
		try {
			var response = blockingStub.databaseOpen(DatabaseOpenRequest.newBuilder()
					.setName(ByteString.copyFrom(name, StandardCharsets.US_ASCII))
					.addAllColumnName(columns.stream().map(
							(column) -> ByteString.copyFrom(column.getName().getBytes(StandardCharsets.US_ASCII)))
							.collect(Collectors.toList()))
					.setLowMemory(lowMemory)
					.build());
			int handle = response.getHandle();
			return new LLRemoteKeyValueDatabase(name, client, handle);
		} catch (StatusRuntimeException ex) {
			throw new IOException(ex);
		}
	}

	@Override
	public LLLuceneIndex getLuceneIndex(String name,
			int instancesCount,
			TextFieldsAnalyzer textFieldsAnalyzer,
			Duration queryRefreshDebounceTime,
			java.time.Duration commitDebounceTime,
			boolean lowMemory) throws IOException {
		try {
			var response = blockingStub.luceneIndexOpen(LuceneIndexOpenRequest.newBuilder()
					.setName(name)
					.setTextFieldsAnalyzer(textFieldsAnalyzer.ordinal())
					.setQueryRefreshDebounceTime((int) queryRefreshDebounceTime.toMillis())
					.setCommitDebounceTime((int) commitDebounceTime.toMillis())
					.setLowMemory(lowMemory)
					.setInstancesCount(instancesCount)
					.build());
			int handle = response.getHandle();
			return new LLRemoteLuceneIndex(client, name, handle, lowMemory, instancesCount);
		} catch (StatusRuntimeException ex) {
			throw new IOException(ex);
		}
	}

	@Override
	public void disconnect() throws IOException {
		try {
			//noinspection ResultOfMethodCallIgnored
			blockingStub.resetConnection(ResetConnectionRequest.newBuilder().build());
			client.shutdown();
		} catch (InterruptedException | StatusRuntimeException e) {
			throw new IOException(e);
		}
	}

	@Override
	public void ping() throws IOException {
		try {
			blockingStub.ping(Empty.newBuilder().build());
		} catch (StatusRuntimeException e) {
			throw new IOException(e);
		}
	}

	@Override
	public double getMediumLatencyMillis() throws IOException {
		int cap = 3;

		long[] results = new long[cap];
		for (int i = 0; i < cap; i++) {
			long time1 = System.nanoTime();
			ping();
			long time2 = System.nanoTime();
			results[i] = time2 - time1;
		}
		return LongStream.of(results).average().orElseThrow() / 1000000;
	}
}
