package com.along101.framework.hystrix.turbine;

import com.netflix.discovery.EurekaClient;
import com.netflix.turbine.discovery.InstanceDiscovery;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.cloud.client.actuator.HasFeatures;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author Spencer Gibb
 */
@Configuration
@EnableConfigurationProperties
@EnableDiscoveryClient
public class TurbineHttpConfiguration {

    @Bean
    public HasFeatures Feature() {
        return HasFeatures.namedFeature("Turbine (HTTP)", TurbineHttpConfiguration.class);
    }

    @Bean
    public ServletRegistrationBean turbineStreamServlet() {
        return new ServletRegistrationBean(new SpringTurbineStreamServlet(), "/turbine.stream");
    }

    @Bean
    public TurbineLifecycle turbineLifecycle(InstanceDiscovery instanceDiscovery) {
        return new TurbineLifecycle(instanceDiscovery);
    }

    @Configuration
    @ConditionalOnClass(EurekaClient.class)
    protected static class EurekaTurbineConfiguration {

        @Bean
        @ConditionalOnMissingBean
        public InstanceDiscovery instanceDiscovery(EurekaClient eurekaClient) {
            return new EurekaInstanceDiscovery(eurekaClient);
        }

    }

}
