package it.cavallium.dbengine.netty;

import io.netty.buffer.ByteBufAllocatorMetric;
import io.netty.buffer.PoolArenaMetric;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocatorMetric;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

public class JMXNettyMonitoringManager {

	private static JMXNettyMonitoringManager instance;

	private final MBeanServer platformMBeanServer;

	private JMXNettyMonitoringManager() {
		this.platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
	}

	public synchronized static void start() {
		if (instance == null) {
			instance = new JMXNettyMonitoringManager();
			instance.startInternal();
		}
	}

	private void startInternal() {
		try {
			int arenaId = 0;
			Map<String, ByteBufAllocatorMetric> allocators = new HashMap<>();
			allocators.put("unpooled", UnpooledByteBufAllocator.DEFAULT.metric());
			allocators.put("pooled", PooledByteBufAllocator.DEFAULT.metric());

			for (var entry : allocators.entrySet()) {
				var name = entry.getKey().replaceAll("[^\\p{IsAlphabetic}\\p{IsDigit}_]", "_");
				var metric = entry.getValue();
				String type;
				StandardMBean mbean;
				if (metric instanceof PooledByteBufAllocatorMetric) {
					var pooledMetric = (PooledByteBufAllocatorMetric) metric;
					for (var arenaEntry : (Iterable<Entry<String, PoolArenaMetric>>) Stream.concat(
							pooledMetric.directArenas().stream().map(arena -> Map.entry("direct", arena)),
							pooledMetric.heapArenas().stream().map(arena -> Map.entry("heap", arena))
					)::iterator) {
						var arenaType = arenaEntry.getKey();
						var arenaMetric = arenaEntry.getValue();
						var jmx = new JMXPoolArenaNettyMonitoring(arenaMetric);
						mbean = new StandardMBean(jmx, JMXPoolArenaNettyMonitoringMBean.class);
						ObjectName botObjectName = new ObjectName("io.netty.stats:name=PoolArena,type=" + arenaType + ",arenaId=" + arenaId++);
						platformMBeanServer.registerMBean(mbean, botObjectName);
					}
					var jmx = new JMXPooledNettyMonitoring(name, pooledMetric);
					type = "pooled";
					mbean = new StandardMBean(jmx, JMXNettyMonitoringMBean.class);
				} else {
					var jmx = new JMXNettyMonitoring(name, metric);
					type = "unpooled";
					mbean = new StandardMBean(jmx, JMXNettyMonitoringMBean.class);
				}

				ObjectName botObjectName = new ObjectName("io.netty.stats:name=ByteBufAllocator,allocatorName=" + name + ",type=" + type);
				platformMBeanServer.registerMBean(mbean, botObjectName);
			}
		} catch (MalformedObjectNameException | NotCompliantMBeanException | InstanceAlreadyExistsException | MBeanRegistrationException e) {
			throw new RuntimeException(e);
		}
	}
}
