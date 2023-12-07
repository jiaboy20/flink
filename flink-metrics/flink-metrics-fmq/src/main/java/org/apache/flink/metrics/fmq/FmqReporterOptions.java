package org.apache.flink.metrics.fmq;

import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.LinkElement;
import org.apache.flink.configuration.description.TextElement;
import org.apache.flink.metrics.MetricConfig;

@Documentation.SuffixOption(ConfigConstants.METRICS_REPORTER_PREFIX + "fmq")
public class FmqReporterOptions {

  public static final ConfigOption<String> BOOTSTRAP_SERVERS = ConfigOptions
    .key("bootstrapServers")
    .stringType()
    .noDefaultValue()
    .withDescription("the fmq broker server host");

  public static final ConfigOption<String> TOPIC = ConfigOptions
    .key("topic")
    .stringType()
    .noDefaultValue()
    .withDescription("the fmq topic to store metrics");

  public static final ConfigOption<String> CURRENT_SITE = ConfigOptions
    .key("currentSite")
    .stringType()
    .noDefaultValue()
    .withDescription("the fmq topic current site");

  public static final ConfigOption<String> JOB_ID =
    ConfigOptions.key("jobId")
      .stringType()
      .defaultValue("")
      .withDescription("The job id under which metrics will be pushed");

  public static final ConfigOption<String> JOB_NAME =
    ConfigOptions.key("jobName")
      .stringType()
      .defaultValue("")
      .withDescription("The job name under which metrics will be pushed");

  public static final ConfigOption<Boolean> RANDOM_JOB_NAME_SUFFIX =
    ConfigOptions.key("randomJobNameSuffix")
      .booleanType()
      .defaultValue(true)
      .withDescription(
        "Specifies whether a random suffix should be appended to the job name.");

  public static final ConfigOption<Boolean> FILTER_LABEL_VALUE_CHARACTER =
    ConfigOptions.key("filterLabelValueCharacters")
      .booleanType()
      .defaultValue(true)
      .withDescription(
        Description.builder()
          .text(
            "Specifies whether to filter label value characters."
              + " If enabled, all characters not matching [a-zA-Z0-9:_] will be removed,"
              + " otherwise no characters will be removed."
              + " Before disabling this option please ensure that your"
              + " label values meet the %s.",
            LinkElement.link(
              "https://prometheus.io/docs/concepts/data_model/#metric-names-and-labels",
              "Prometheus requirements"))
          .build());

  public static final ConfigOption<String> GROUPING_KEY =
    ConfigOptions.key("groupingKey")
      .stringType()
      .defaultValue("")
      .withDescription(
        Description.builder()
          .text(
            "Specifies the grouping key which is the group and global labels of all metrics."
              + " The label name and value are separated by '=', and labels are separated by ';', e.g., %s."
              + " Please ensure that your grouping key meets the %s.",
            TextElement.code("k1=v1;k2=v2"),
            LinkElement.link(
              "https://prometheus.io/docs/concepts/data_model/#metric-names-and-labels",
              "Prometheus requirements"))
          .build());

  static String getString(MetricConfig config, ConfigOption<String> key) {
    return config.getString(key.key(), key.defaultValue());
  }

  static int getInteger(MetricConfig config, ConfigOption<Integer> key) {
    return config.getInteger(key.key(), key.defaultValue());
  }

  static boolean getBoolean(MetricConfig config, ConfigOption<Boolean> key) {
    return config.getBoolean(key.key(), key.defaultValue());
  }
}
