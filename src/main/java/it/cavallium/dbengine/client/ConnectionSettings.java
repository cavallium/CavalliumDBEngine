package it.cavallium.dbengine.client;

import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import java.net.SocketAddress;
import java.nio.file.Path;
import java.util.Map;
import org.jetbrains.annotations.Nullable;

public sealed interface ConnectionSettings {

	sealed interface PrimaryConnectionSettings extends ConnectionSettings {}

	sealed interface SubConnectionSettings extends ConnectionSettings {}

	record MemoryConnectionSettings() implements PrimaryConnectionSettings, SubConnectionSettings {}

	record LocalConnectionSettings(Path dataPath) implements PrimaryConnectionSettings, SubConnectionSettings {}

	record QuicConnectionSettings(SocketAddress bindAddress, SocketAddress remoteAddress) implements
			PrimaryConnectionSettings, SubConnectionSettings {}

	record MultiConnectionSettings(Map<ConnectionPart, SubConnectionSettings> parts) implements
			PrimaryConnectionSettings {

		public Multimap<SubConnectionSettings, ConnectionPart> getConnections() {
			Multimap<SubConnectionSettings, ConnectionPart> result = com.google.common.collect.HashMultimap.create();
			parts.forEach((connectionPart, subConnectionSettings) -> result.put(subConnectionSettings,connectionPart));
			return Multimaps.unmodifiableMultimap(result);
		}
	}

	sealed interface ConnectionPart {

		record ConnectionPartLucene(@Nullable String name) implements ConnectionPart {}

		record ConnectionPartRocksDB(@Nullable String name) implements ConnectionPart {}
	}
}
