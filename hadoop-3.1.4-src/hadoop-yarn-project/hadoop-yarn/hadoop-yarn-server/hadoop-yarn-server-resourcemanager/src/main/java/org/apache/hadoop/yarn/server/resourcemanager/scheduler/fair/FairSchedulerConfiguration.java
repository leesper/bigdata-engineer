/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import static org.apache.hadoop.yarn.util.resource.ResourceUtils.RESOURCE_REQUEST_VALUE_PATTERN;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.UnitsConversionUtil;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;

@Private
@Evolving
public class FairSchedulerConfiguration extends Configuration {

  public static final Log LOG = LogFactory.getLog(
      FairSchedulerConfiguration.class.getName());

  /**
   * Resource Increment request grant-able by the FairScheduler.
   * This property is looked up in the yarn-site.xml.
   * @deprecated The preferred way to configure the increment is by using the
   * yarn.resource-types.{RESOURCE_NAME}.increment-allocation property,
   * for memory: yarn.resource-types.memory-mb.increment-allocation
   */
  @Deprecated
  public static final String RM_SCHEDULER_INCREMENT_ALLOCATION_MB =
    YarnConfiguration.YARN_PREFIX + "scheduler.increment-allocation-mb";
  @Deprecated
  public static final int DEFAULT_RM_SCHEDULER_INCREMENT_ALLOCATION_MB = 1024;
  /**
   * Resource Increment request grant-able by the FairScheduler.
   * This property is looked up in the yarn-site.xml.
   * @deprecated The preferred way to configure the increment is by using the
   * yarn.resource-types.{RESOURCE_NAME}.increment-allocation property,
   * for CPU: yarn.resource-types.vcores.increment-allocation
   */
  @Deprecated
  public static final String RM_SCHEDULER_INCREMENT_ALLOCATION_VCORES =
    YarnConfiguration.YARN_PREFIX + "scheduler.increment-allocation-vcores";
  @Deprecated
  public static final int DEFAULT_RM_SCHEDULER_INCREMENT_ALLOCATION_VCORES = 1;

  /** Threshold for container size for making a container reservation as a
   * multiple of increment allocation. Only container sizes above this are
   * allowed to reserve a node */
  public static final String
      RM_SCHEDULER_RESERVATION_THRESHOLD_INCREMENT_MULTIPLE =
      YarnConfiguration.YARN_PREFIX +
          "scheduler.reservation-threshold.increment-multiple";
  public static final float
      DEFAULT_RM_SCHEDULER_RESERVATION_THRESHOLD_INCREMENT_MULTIPLE = 2f;

  private static final String CONF_PREFIX =  "yarn.scheduler.fair.";

  public static final String ALLOCATION_FILE = CONF_PREFIX + "allocation.file";
  protected static final String DEFAULT_ALLOCATION_FILE = "fair-scheduler.xml";
  
  /** Whether pools can be created that were not specified in the FS configuration file
   */
  protected static final String ALLOW_UNDECLARED_POOLS = CONF_PREFIX + "allow-undeclared-pools";
  protected static final boolean DEFAULT_ALLOW_UNDECLARED_POOLS = true;
  
  /** Whether to use the user name as the queue name (instead of "default") if
   * the request does not specify a queue. */
  protected static final String  USER_AS_DEFAULT_QUEUE = CONF_PREFIX + "user-as-default-queue";
  protected static final boolean DEFAULT_USER_AS_DEFAULT_QUEUE = true;

  protected static final float  DEFAULT_LOCALITY_THRESHOLD = -1.0f;

  /** Cluster threshold for node locality. */
  protected static final String LOCALITY_THRESHOLD_NODE = CONF_PREFIX + "locality.threshold.node";
  protected static final float  DEFAULT_LOCALITY_THRESHOLD_NODE =
		  DEFAULT_LOCALITY_THRESHOLD;

  /** Cluster threshold for rack locality. */
  protected static final String LOCALITY_THRESHOLD_RACK = CONF_PREFIX + "locality.threshold.rack";
  protected static final float  DEFAULT_LOCALITY_THRESHOLD_RACK =
		  DEFAULT_LOCALITY_THRESHOLD;

  /**
   * Delay for node locality.
   * @deprecated Continuous scheduling is known to cause locking issue inside
   * Only used when {@link #CONTINUOUS_SCHEDULING_ENABLED} is enabled
   */
  @Deprecated
  protected static final String LOCALITY_DELAY_NODE_MS = CONF_PREFIX +
      "locality-delay-node-ms";
  @Deprecated
  protected static final long DEFAULT_LOCALITY_DELAY_NODE_MS = -1L;

  /**
   * Delay for rack locality.
   * @deprecated Continuous scheduling is known to cause locking issue inside
   * Only used when {@link #CONTINUOUS_SCHEDULING_ENABLED} is enabled
   */
  @Deprecated
  protected static final String LOCALITY_DELAY_RACK_MS = CONF_PREFIX +
      "locality-delay-rack-ms";
  @Deprecated
  protected static final long DEFAULT_LOCALITY_DELAY_RACK_MS = -1L;

  /**
   * Enable continuous scheduling or not.
   * @deprecated Continuous scheduling is known to cause locking issue inside
   * the scheduler in larger cluster, more than 100 nodes, use
   * {@link #ASSIGN_MULTIPLE} to improve  container allocation ramp up.
   */
  @Deprecated
  protected static final String CONTINUOUS_SCHEDULING_ENABLED = CONF_PREFIX +
      "continuous-scheduling-enabled";
  @Deprecated
  protected static final boolean DEFAULT_CONTINUOUS_SCHEDULING_ENABLED = false;

  /**
   * Sleep time of each pass in continuous scheduling (5ms in default).
   * @deprecated Continuous scheduling is known to cause locking issue inside
   * Only used when {@link #CONTINUOUS_SCHEDULING_ENABLED} is enabled
   */
  @Deprecated
  protected static final String CONTINUOUS_SCHEDULING_SLEEP_MS = CONF_PREFIX +
      "continuous-scheduling-sleep-ms";
  @Deprecated
  protected static final int DEFAULT_CONTINUOUS_SCHEDULING_SLEEP_MS = 5;

  /** Whether preemption is enabled. */
  protected static final String  PREEMPTION = CONF_PREFIX + "preemption";
  protected static final boolean DEFAULT_PREEMPTION = false;

  protected static final String PREEMPTION_THRESHOLD =
      CONF_PREFIX + "preemption.cluster-utilization-threshold";
  protected static final float DEFAULT_PREEMPTION_THRESHOLD = 0.8f;

  protected static final String WAIT_TIME_BEFORE_KILL = CONF_PREFIX + "waitTimeBeforeKill";
  protected static final int DEFAULT_WAIT_TIME_BEFORE_KILL = 15000;

  /**
   * Postfix for resource allocation increments in the
   * yarn.resource-types.{RESOURCE_NAME}.increment-allocation property.
   */
  static final String INCREMENT_ALLOCATION = ".increment-allocation";

  /**
   * Configurable delay (ms) before an app's starvation is considered after
   * it is identified. This is to give the scheduler enough time to
   * allocate containers post preemption. This delay is added to the
   * {@link #WAIT_TIME_BEFORE_KILL} and enough heartbeats.
   *
   * This is intended to be a backdoor on production clusters, and hence
   * intentionally not documented.
   */
  protected static final String WAIT_TIME_BEFORE_NEXT_STARVATION_CHECK_MS =
      CONF_PREFIX + "waitTimeBeforeNextStarvationCheck";
  protected static final long
      DEFAULT_WAIT_TIME_BEFORE_NEXT_STARVATION_CHECK_MS = 10000;

  /** Whether to assign multiple containers in one check-in. */
  public static final String  ASSIGN_MULTIPLE = CONF_PREFIX + "assignmultiple";
  protected static final boolean DEFAULT_ASSIGN_MULTIPLE = false;

  /** Whether to give more weight to apps requiring many resources. */
  protected static final String  SIZE_BASED_WEIGHT = CONF_PREFIX + "sizebasedweight";
  protected static final boolean DEFAULT_SIZE_BASED_WEIGHT = false;

  /** Maximum number of containers to assign on each check-in. */
  public static final String DYNAMIC_MAX_ASSIGN =
      CONF_PREFIX + "dynamic.max.assign";
  private static final boolean DEFAULT_DYNAMIC_MAX_ASSIGN = true;

  /**
   * Specify exact number of containers to assign on each heartbeat, if dynamic
   * max assign is turned off.
   */
  protected static final String MAX_ASSIGN = CONF_PREFIX + "max.assign";
  protected static final int DEFAULT_MAX_ASSIGN = -1;

  /** The update interval for calculating resources in FairScheduler .*/
  public static final String UPDATE_INTERVAL_MS =
      CONF_PREFIX + "update-interval-ms";
  public static final int DEFAULT_UPDATE_INTERVAL_MS = 500;

  /** Ratio of nodes available for an app to make an reservation on. */
  public static final String RESERVABLE_NODES =
          CONF_PREFIX + "reservable-nodes";
  public static final float RESERVABLE_NODES_DEFAULT = 0.05f;

  public FairSchedulerConfiguration() {
    super();
  }
  
  public FairSchedulerConfiguration(Configuration conf) {
    super(conf);
  }

  public Resource getMinimumAllocation() {
    int mem = getInt(
        YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);
    int cpu = getInt(
        YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
    return Resources.createResource(mem, cpu);
  }

  public Resource getMaximumAllocation() {
    int mem = getInt(
        YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB);
    int cpu = getInt(
        YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES);
    return Resources.createResource(mem, cpu);
  }

  public Resource getIncrementAllocation() {
    Long memory = null;
    Integer vCores = null;
    Map<String, Long> others = new HashMap<>();
    ResourceInformation[] resourceTypes = ResourceUtils.getResourceTypesArray();
    for (int i=0; i < resourceTypes.length; ++i) {
      String name = resourceTypes[i].getName();
      String propertyKey = getAllocationIncrementPropKey(name);
      String propValue = get(propertyKey);
      if (propValue != null) {
        Matcher matcher = RESOURCE_REQUEST_VALUE_PATTERN.matcher(propValue);
        if (matcher.matches()) {
          long value = Long.parseLong(matcher.group(1));
          String unit = matcher.group(2);
          long valueInDefaultUnits = getValueInDefaultUnits(value, unit, name);
          others.put(name, valueInDefaultUnits);
        } else {
          throw new IllegalArgumentException("Property " + propertyKey +
              " is not in \"value [unit]\" format: " + propValue);
        }
      }
    }
    if (others.containsKey(ResourceInformation.MEMORY_MB.getName())) {
      memory = others.get(ResourceInformation.MEMORY_MB.getName());
      if (get(RM_SCHEDULER_INCREMENT_ALLOCATION_MB) != null) {
        String overridingKey = getAllocationIncrementPropKey(
                ResourceInformation.MEMORY_MB.getName());
        LOG.warn("Configuration " + overridingKey + "=" + get(overridingKey) +
            " is overriding the " + RM_SCHEDULER_INCREMENT_ALLOCATION_MB +
            "=" + get(RM_SCHEDULER_INCREMENT_ALLOCATION_MB) + " property");
      }
      others.remove(ResourceInformation.MEMORY_MB.getName());
    } else {
      memory = getLong(
          RM_SCHEDULER_INCREMENT_ALLOCATION_MB,
          DEFAULT_RM_SCHEDULER_INCREMENT_ALLOCATION_MB);
    }
    if (others.containsKey(ResourceInformation.VCORES.getName())) {
      vCores = others.get(ResourceInformation.VCORES.getName()).intValue();
      if (get(RM_SCHEDULER_INCREMENT_ALLOCATION_VCORES) != null) {
        String overridingKey = getAllocationIncrementPropKey(
            ResourceInformation.VCORES.getName());
        LOG.warn("Configuration " + overridingKey + "=" + get(overridingKey) +
            " is overriding the " + RM_SCHEDULER_INCREMENT_ALLOCATION_VCORES +
            "=" + get(RM_SCHEDULER_INCREMENT_ALLOCATION_VCORES) + " property");
      }
      others.remove(ResourceInformation.VCORES.getName());
    } else {
      vCores = getInt(
          RM_SCHEDULER_INCREMENT_ALLOCATION_VCORES,
          DEFAULT_RM_SCHEDULER_INCREMENT_ALLOCATION_VCORES);
    }
    return Resource.newInstance(memory, vCores, others);
  }

  private long getValueInDefaultUnits(long value, String unit,
      String resourceName) {
    return unit.isEmpty() ? value : UnitsConversionUtil.convert(unit,
        ResourceUtils.getDefaultUnit(resourceName), value);
  }

  private String getAllocationIncrementPropKey(String resourceName) {
    return YarnConfiguration.RESOURCE_TYPES + "." + resourceName +
        INCREMENT_ALLOCATION;
  }

  public float getReservationThresholdIncrementMultiple() {
    return getFloat(
      RM_SCHEDULER_RESERVATION_THRESHOLD_INCREMENT_MULTIPLE,
      DEFAULT_RM_SCHEDULER_RESERVATION_THRESHOLD_INCREMENT_MULTIPLE);
  }

  public float getLocalityThresholdNode() {
    return getFloat(LOCALITY_THRESHOLD_NODE, DEFAULT_LOCALITY_THRESHOLD_NODE);
  }

  public float getLocalityThresholdRack() {
    return getFloat(LOCALITY_THRESHOLD_RACK, DEFAULT_LOCALITY_THRESHOLD_RACK);
  }

  /**
   * Whether continuous scheduling is turned on.
   * @deprecated use {@link #ASSIGN_MULTIPLE} to improve container allocation
   * ramp up.
   * @return whether continuous scheduling is enabled
   */
  @Deprecated
  public boolean isContinuousSchedulingEnabled() {
    return getBoolean(CONTINUOUS_SCHEDULING_ENABLED,
        DEFAULT_CONTINUOUS_SCHEDULING_ENABLED);
  }

  /**
   * The sleep time of the continuous scheduler thread.
   * @deprecated linked to {@link #CONTINUOUS_SCHEDULING_ENABLED} deprecation
   * @return sleep time in ms
   */
  @Deprecated
  public int getContinuousSchedulingSleepMs() {
    return getInt(CONTINUOUS_SCHEDULING_SLEEP_MS,
        DEFAULT_CONTINUOUS_SCHEDULING_SLEEP_MS);
  }

  /**
   * Delay in milliseconds for locality fallback node to rack.
   * @deprecated linked to {@link #CONTINUOUS_SCHEDULING_ENABLED} deprecation
   * @return delay in ms
   */
  @Deprecated
  public long getLocalityDelayNodeMs() {
    return getLong(LOCALITY_DELAY_NODE_MS, DEFAULT_LOCALITY_DELAY_NODE_MS);
  }

  /**
   * Delay in milliseconds for locality fallback rack to other.
   * @deprecated linked to {@link #CONTINUOUS_SCHEDULING_ENABLED} deprecation
   * @return delay in ms
   */
  @Deprecated
  public long getLocalityDelayRackMs() {
    return getLong(LOCALITY_DELAY_RACK_MS, DEFAULT_LOCALITY_DELAY_RACK_MS);
  }

  public boolean getPreemptionEnabled() {
    return getBoolean(PREEMPTION, DEFAULT_PREEMPTION);
  }

  public float getPreemptionUtilizationThreshold() {
    return getFloat(PREEMPTION_THRESHOLD, DEFAULT_PREEMPTION_THRESHOLD);
  }

  public boolean getAssignMultiple() {
    return getBoolean(ASSIGN_MULTIPLE, DEFAULT_ASSIGN_MULTIPLE);
  }

  public boolean isMaxAssignDynamic() {
    return getBoolean(DYNAMIC_MAX_ASSIGN, DEFAULT_DYNAMIC_MAX_ASSIGN);
  }

  public int getMaxAssign() {
    return getInt(MAX_ASSIGN, DEFAULT_MAX_ASSIGN);
  }

  public boolean getSizeBasedWeight() {
    return getBoolean(SIZE_BASED_WEIGHT, DEFAULT_SIZE_BASED_WEIGHT);
  }

  public long getWaitTimeBeforeNextStarvationCheck() {
    return getLong(WAIT_TIME_BEFORE_NEXT_STARVATION_CHECK_MS,
        DEFAULT_WAIT_TIME_BEFORE_NEXT_STARVATION_CHECK_MS);
  }
  
  public int getWaitTimeBeforeKill() {
    return getInt(WAIT_TIME_BEFORE_KILL, DEFAULT_WAIT_TIME_BEFORE_KILL);
  }

  public boolean getUsePortForNodeName() {
    return getBoolean(YarnConfiguration.RM_SCHEDULER_INCLUDE_PORT_IN_NODE_NAME,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_USE_PORT_FOR_NODE_NAME);
  }

  public float getReservableNodes() {
    return getFloat(RESERVABLE_NODES, RESERVABLE_NODES_DEFAULT);
  }

  /**
   * Parses a resource config value of a form like "1024", "1024 mb",
   * or "1024 mb, 3 vcores". If no units are given, megabytes are assumed.
   * 
   * @throws AllocationConfigurationException
   */
  public static ConfigurableResource parseResourceConfigValue(String val)
      throws AllocationConfigurationException {
    ConfigurableResource configurableResource;
    try {
      val = StringUtils.toLowerCase(val);
      if (val.contains("%")) {
        configurableResource = new ConfigurableResource(
            getResourcePercentage(val));
      } else {
        int memory = findResource(val, "mb");
        int vcores = findResource(val, "vcores");
        configurableResource = new ConfigurableResource(
            BuilderUtils.newResource(memory, vcores));
      }
    } catch (AllocationConfigurationException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new AllocationConfigurationException(
          "Error reading resource config", ex);
    }
    return configurableResource;
  }

  private static double[] getResourcePercentage(
      String val) throws AllocationConfigurationException {
    int numberOfKnownResourceTypes = ResourceUtils
        .getNumberOfKnownResourceTypes();
    double[] resourcePercentage = new double[numberOfKnownResourceTypes];
    String[] strings = val.split(",");
    if (strings.length == 1) {
      double percentage = findPercentage(strings[0], "");
      for (int i = 0; i < numberOfKnownResourceTypes; i++) {
        resourcePercentage[i] = percentage/100;
      }
    } else {
      resourcePercentage[0] = findPercentage(val, "memory")/100;
      resourcePercentage[1] = findPercentage(val, "cpu")/100;
    }
    return resourcePercentage;
  }

  private static double findPercentage(String val, String units)
    throws AllocationConfigurationException {
    final Pattern pattern =
        Pattern.compile("((\\d+)(\\.\\d*)?)\\s*%\\s*" + units);
    Matcher matcher = pattern.matcher(val);
    if (!matcher.find()) {
      if (units.equals("")) {
        throw new AllocationConfigurationException("Invalid percentage: " +
            val);
      } else {
        throw new AllocationConfigurationException("Missing resource: " +
            units);
      }
    }
    return Double.parseDouble(matcher.group(1));
  }

  public long getUpdateInterval() {
    return getLong(UPDATE_INTERVAL_MS, DEFAULT_UPDATE_INTERVAL_MS);
  }
  
  private static int findResource(String val, String units)
      throws AllocationConfigurationException {
    final Pattern pattern = Pattern.compile("(\\d+)(\\.\\d*)?\\s*" + units);
    Matcher matcher = pattern.matcher(val);
    if (!matcher.find()) {
      throw new AllocationConfigurationException("Missing resource: " + units);
    }
    return Integer.parseInt(matcher.group(1));
  }
}
