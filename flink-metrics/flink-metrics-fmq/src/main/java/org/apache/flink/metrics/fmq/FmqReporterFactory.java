package org.apache.flink.metrics.fmq;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.reporter.MetricReporterFactory;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.metrics.fmq.FmqReporterOptions.*;

public class FmqReporterFactory implements MetricReporterFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(FmqReporterFactory.class);

  @Override
  public FmqReporter createMetricReporter(Properties properties) {
    MetricConfig metricConfig = (MetricConfig) properties;
    String bootstrapServers = FmqReporterOptions.getString(metricConfig, BOOTSTRAP_SERVERS);
    String topic = FmqReporterOptions.getString(metricConfig, TOPIC);
    String configuredJobId = FmqReporterOptions.getString(metricConfig, JOB_ID);
    String configuredJobName = FmqReporterOptions.getString(metricConfig, JOB_NAME);
    boolean randomSuffix = FmqReporterOptions.getBoolean(metricConfig, RANDOM_JOB_NAME_SUFFIX);
    Map<String, String> groupingKey = parseGroupingKey(FmqReporterOptions.getString(metricConfig, GROUPING_KEY));
    String jobName = configuredJobName;
    if (randomSuffix) {
      jobName = configuredJobName + new AbstractID();
    }
    String currentSite =  FmqReporterOptions.getString(metricConfig, CURRENT_SITE);
    LOGGER.info("Configured FmqReporter with {bootstrapServers:{}, topic:{}, currentSite:{}, jobName:{}, randomJobNameSuffix:{}, groupingKey:{}}", bootstrapServers, topic, currentSite, jobName, randomSuffix, groupingKey);
    return new FmqReporter(bootstrapServers, topic, currentSite, configuredJobId, jobName, groupingKey);
  }

  @VisibleForTesting
  static Map<String, String> parseGroupingKey(final String groupingKeyConfig) {
    if (!groupingKeyConfig.isEmpty()) {
      Map<String, String> groupingKey = new HashMap<>();
      String[] kvs = groupingKeyConfig.split(";");
      for (String kv : kvs) {
        int idx = kv.indexOf("=");
        if (idx < 0) {
          LOGGER.warn("Invalid FmqReporter groupingKey:{}, will be ignored", kv);
          continue;
        }
        String labelKey = kv.substring(0, idx);
        String labelValue = kv.substring(idx + 1);
        if (StringUtils.isNullOrWhitespaceOnly(labelKey)
          || StringUtils.isNullOrWhitespaceOnly(labelValue)) {
          LOGGER.warn("Invalid groupingKey {labelKey:{}, labelValue:{}} must not be empty", labelKey, labelValue);
          continue;
        }
        groupingKey.put(labelKey, labelValue);
      }
      return groupingKey;
    }
    return Collections.emptyMap();
  }
}
