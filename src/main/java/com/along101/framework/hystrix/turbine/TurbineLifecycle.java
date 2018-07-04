package com.along101.framework.hystrix.turbine;

import org.springframework.context.SmartLifecycle;
import org.springframework.core.Ordered;

import com.netflix.turbine.discovery.InstanceDiscovery;
import com.netflix.turbine.init.TurbineInit;
import com.netflix.turbine.plugins.PluginsFactory;

/**
 * @author Spencer Gibb
 */
public class TurbineLifecycle implements SmartLifecycle, Ordered {

	private final InstanceDiscovery instanceDiscovery;

	private boolean running;

	public TurbineLifecycle(InstanceDiscovery instanceDiscovery) {
		this.instanceDiscovery = instanceDiscovery;
	}

	@Override
	public boolean isAutoStartup() {
		return true;
	}

	@Override
	public void stop(Runnable callback) {
		callback.run();
	}

	@Override
	public void start() {
		PluginsFactory.setClusterMonitorFactory(new SpringAggregatorFactory());
		PluginsFactory.setInstanceDiscovery(instanceDiscovery);
		TurbineInit.init();
	}

	@Override
	public void stop() {
		this.running = false;
	}

	@Override
	public boolean isRunning() {
		return this.running;
	}

	@Override
	public int getPhase() {
		return 0;
	}

	@Override
	public int getOrder() {
		return -1;
	}

}
