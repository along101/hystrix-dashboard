package com.along101.framework.hystrix.turbine;

import com.google.common.collect.Lists;
import com.netflix.appinfo.AmazonInfo;
import com.netflix.appinfo.DataCenterInfo;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.EurekaClient;
import com.netflix.discovery.shared.Application;
import com.netflix.discovery.shared.Applications;
import com.netflix.turbine.discovery.Instance;
import com.netflix.turbine.discovery.InstanceDiscovery;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class EurekaInstanceDiscovery implements InstanceDiscovery {

    private static final Logger logger = LoggerFactory.getLogger(EurekaInstanceDiscovery.class);
    private final EurekaClient eurekaClient;

    public EurekaInstanceDiscovery(EurekaClient eurekaClient) {
        this.eurekaClient = eurekaClient;
    }

    @Override
    public Collection<Instance> getInstanceList() throws Exception {

        List<Instance> instances = new ArrayList<Instance>();

        List<String> appNames = parseApps();
        if (appNames == null || appNames.size() == 0) {
            logger.info("No apps configured, returning an empty instance list");
            return instances;
        }

        logger.info("Fetching instance list for apps:" + appNames);

        for (String appName : appNames) {
            try {
                instances.addAll(getInstancesForApp(appName));
            } catch (Exception e) {
                logger.error("Failed to fetch instances for app: " + appName + ", retrying once more", e);
                try {
                    instances.addAll(getInstancesForApp(appName));
                } catch (Exception e1) {
                    logger.error("Failed again to fetch instances for app: " + appName + ", giving up", e);
                }
            }
        }
        return instances;
    }

    private List<Instance> getInstancesForApp(String appName) throws Exception {

        List<Instance> instances = new ArrayList<Instance>();

        logger.info("Fetching instances for app:------------- " + appName);
        Application app = eurekaClient.getApplication(appName);
        List<InstanceInfo> instancesForApp = app.getInstances();

        if (instancesForApp != null) {
            logger.info("Received instance list for app: " + appName + " = " + instancesForApp.size());
            for (InstanceInfo iInfo : instancesForApp) {
                Instance instance = marshallInstanceInfo(iInfo);
                if (instance != null) {
                    logger.info("{},{}", instance.getCluster(), instance.getHostname());
                    instances.add(instance);
                }
            }
        }

        return instances;
    }

    protected Instance marshallInstanceInfo(InstanceInfo iInfo) {

        String hostname = iInfo.getIPAddr();
        String cluster = getClusterName(iInfo);
        Boolean status = parseInstanceStatus(iInfo.getStatus());

        if (hostname != null && cluster != null && status != null) {
            Instance instance = new Instance(hostname, cluster, status);
            Map<String, String> metadata = iInfo.getMetadata();
            if (metadata != null) {
                instance.getAttributes().putAll(metadata);
            }

            String asgName = iInfo.getASGName();
            if (asgName != null) {
                instance.getAttributes().put("asg", asgName);
            }

            DataCenterInfo dcInfo = iInfo.getDataCenterInfo();
            if (dcInfo != null && dcInfo.getName().equals(DataCenterInfo.Name.Amazon)) {
                AmazonInfo amznInfo = (AmazonInfo) dcInfo;
                instance.getAttributes().putAll(amznInfo.getMetadata());
            }

            return instance;
        } else {
            return null;
        }
    }

    /**
     * Helper that returns whether the instance is Up of Down
     *
     * @param status
     * @return
     */
    protected Boolean parseInstanceStatus(InstanceInfo.InstanceStatus status) {

        if (status != null) {
            if (status == InstanceInfo.InstanceStatus.UP) {
                return Boolean.TRUE;
            } else {
                return Boolean.FALSE;
            }
        } else {
            return null;
        }
    }

    protected String getClusterName(InstanceInfo iInfo) {
        String clusterName = iInfo.getMetadata().get("turbine.cluster");
        if (!StringUtils.isBlank(clusterName)) {
            return clusterName;
        }
        return iInfo.getASGName();
    }

    private List<String> parseApps() {
        List<String> list = Lists.newArrayList();
        Applications applications = eurekaClient.getApplications();
        if (applications != null) {
            for (Application app : applications.getRegisteredApplications()) {
                list.add(app.getName());
            }
        }
        return list;
    }

}
