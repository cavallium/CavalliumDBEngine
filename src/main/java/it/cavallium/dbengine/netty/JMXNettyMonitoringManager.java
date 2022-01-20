package it.cavallium.dbengine.netty;

import io.net5.buffer.api.BufferAllocator;
import io.net5.buffer.api.DefaultBufferAllocators;
import io.net5.buffer.api.pool.MetricUtils;
import io.net5.buffer.api.pool.PoolArenaMetric;
import io.net5.buffer.api.pool.PooledBufferAllocator;
import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

public class JMXNettyMonitoringManager {

	private final AtomicInteger nextArenaId = new AtomicInteger();
	private static JMXNettyMonitoringManager instance;

	private final MBeanServer platformMBeanServer;

	private JMXNettyMonitoringManager() {
		this.platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
	}

	public static void initialize() {
		var instance = getInstance();
		instance.register("global", DefaultBufferAllocators.preferredAllocator());
	}

	public synchronized static JMXNettyMonitoringManager getInstance() {
		if (instance == null) {
			instance = new JMXNettyMonitoringManager();
		}
		return instance;
	}

	public void register(String name, BufferAllocator allocator) {
		try {
			name = name.replaceAll("[^\\p{IsAlphabetic}\\p{IsDigit}_]", "_");
			String type;
			StandardMBean mbean;
			if (allocator instanceof PooledBufferAllocator pooledAllocator) {
				for (var arenaMetric : MetricUtils.getPoolArenaMetrics(pooledAllocator)) {
					String arenaType = pooledAllocator.isDirectBufferPooled() ? "direct" : "heap";
					var jmx = new JMXPoolArenaNettyMonitoring(arenaMetric);
					mbean = new StandardMBean(jmx, JMXPoolArenaNettyMonitoringMBean.class);
					ObjectName botObjectName = new ObjectName("io.netty.stats:name=PoolArena,type=" + arenaType + ",arenaId=" + nextArenaId.getAndIncrement());
					platformMBeanServer.registerMBean(mbean, botObjectName);
				}
				var jmx = new JMXPooledNettyMonitoring(name, pooledAllocator);
				type = "pooled";
				mbean = new StandardMBean(jmx, JMXNettyMonitoringMBean.class);
				ObjectName botObjectName = new ObjectName("io.netty.stats:name=ByteBufAllocator,allocatorName=" + name + ",type=" + type);
				platformMBeanServer.registerMBean(mbean, botObjectName);
			}

		} catch (MalformedObjectNameException | NotCompliantMBeanException | InstanceAlreadyExistsException | MBeanRegistrationException e) {
			throw new RuntimeException(e);
		}
	}
}
