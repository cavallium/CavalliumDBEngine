package it.cavallium.dbengine.netty;

import io.netty5.buffer.api.BufferAllocator;
import io.netty5.buffer.api.pool.MetricUtils;
import io.netty5.buffer.api.pool.PoolArenaMetric;
import io.netty5.buffer.api.pool.PooledBufferAllocator;
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
		getInstance();
	}

	public synchronized static JMXNettyMonitoringManager getInstance() {
		if (instance == null) {
			instance = new JMXNettyMonitoringManager();
		}
		return instance;
	}

	public void register(String name, BufferAllocator metric) {
		try {
			name = name.replaceAll("[^\\p{IsAlphabetic}\\p{IsDigit}_]", "_");
			String type;
			StandardMBean mbean;
			if (metric instanceof PooledBufferAllocator pooledMetric) {
				for (var arenaMetric : MetricUtils.getPoolArenaMetrics(pooledMetric)) {
					String arenaType = pooledMetric.isDirectBufferPooled() ? "direct" : "heap";
					var jmx = new JMXPoolArenaNettyMonitoring(arenaMetric);
					mbean = new StandardMBean(jmx, JMXPoolArenaNettyMonitoringMBean.class);
					ObjectName botObjectName = new ObjectName("io.netty.stats:name=PoolArena,type=" + arenaType + ",arenaId=" + nextArenaId.getAndIncrement());
					platformMBeanServer.registerMBean(mbean, botObjectName);
				}
				var jmx = new JMXPooledNettyMonitoring(name, pooledMetric);
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
