package it.cavallium.dbengine.database.remote.server;

import java.io.IOException;
import java.nio.file.Paths;
import it.cavallium.dbengine.database.disk.LLLocalDatabaseConnection;

public class Main {

	/**
	 * @param args [database-path] [host] [port] [cert-chain-file-path] [private-key-file-path]
	 *             [trust-cert-collection-file-path]
	 */
	public static void main(String[] args) throws IOException, InterruptedException {
		if (args.length != 7) {
			System.out.println(
					"Usage: java -jar dataserver.jar <database-path> <host> <port> <cert-chain-file-path> <private-key-file-path> <trust-cert-collection-file-path> <crash-if-wal-errored>");
		} else {
			System.out.println("Database server starting...");
			var dbConnection = new LLLocalDatabaseConnection(Paths.get(args[0]),
					Boolean.parseBoolean(args[6]));
			dbConnection.connect();
			var serverManager = new DbServerManager(dbConnection, args[1], Integer.parseInt(args[2]),
					Paths.get(args[3]), Paths.get(args[4]), Paths.get(args[5]));
			serverManager.start();
			serverManager.blockUntilShutdown();
			System.out.println("Database has been terminated.");
		}
	}
}
