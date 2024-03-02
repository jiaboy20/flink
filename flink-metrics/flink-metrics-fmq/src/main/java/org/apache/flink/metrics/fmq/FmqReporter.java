package org.apache.flink.metrics.fmq;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.metrics.LogicalScopeProvider;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import com.focustech.fmq.clients.producer.ProducerMessage;
import com.focustech.fmq.clients.producer.SendResult;
import com.focustech.fmq.clients.producer.config.ProducerConfig;
import com.focustech.fmq.clients.producer.impl.FMQProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.apache.flink.metrics.fmq.FmqReporterOptions.FILTER_LABEL_VALUE_CHARACTER;

/** fmq reporter for flink metrics. */
public class FmqReporter implements MetricReporter, Scheduled {

    protected final Logger log = LoggerFactory.getLogger(getClass());
    private static final Pattern UNALLOWED_CHAR_PATTERN = Pattern.compile("[^a-zA-Z0-9:_]");
    private static final CharacterFilter CHARACTER_FILTER = FmqReporter::replaceInvalidChars;
    private CharacterFilter labelValueCharactersFilter = CHARACTER_FILTER;
    private static final List<Double> HISTORGRAM_QUANTILES =
            Arrays.asList(.5, .75, .95, .98, .99, .999);
    @VisibleForTesting static final char SCOPE_SEPARATOR = '_';
    @VisibleForTesting static final String SCOPE_PREFIX = "flink" + SCOPE_SEPARATOR;

    private final ObjectMapper jsonMapper;
    private final String jobId;
    private final String jobName;
    private final Map<String, String> groupingKey;
    @VisibleForTesting private final FMQProducer fmqProducer;
    private final String topic;

    private final Map<Gauge<?>, FmqMetricInfo> gaugeMetrics = new HashMap<>();
    private final Map<Counter, FmqMetricInfo> counterMetrics = new HashMap<>();
    private final Map<Histogram, FmqMetricInfo> histogramMetrics = new HashMap<>();
    private final Map<Meter, FmqMetricInfo> meterMetrics = new HashMap<>();

    @VisibleForTesting
    static String replaceInvalidChars(final String input) {
        // https://prometheus.io/docs/instrumenting/writing_exporters/
        // Only [a-zA-Z0-9:_] are valid in metric names, any other characters should be sanitized to
        // an underscore.
        return UNALLOWED_CHAR_PATTERN.matcher(input).replaceAll("_");
    }

    public FmqReporter(
            String bootstrapServers,
            String topic,
            String currentSite,
            String jobId,
            String jobName,
            Map<String, String> groupingKey) {
        this.topic = topic;
        this.jobId = jobId;
        this.jobName = Preconditions.checkNotNull(jobName);
        this.groupingKey = Preconditions.checkNotNull(groupingKey);
        ProducerConfig fmqConfig = new ProducerConfig(bootstrapServers);
        fmqConfig.setCurrentSite(currentSite);
        this.fmqProducer = new FMQProducer(fmqConfig);
        this.jsonMapper = JacksonMapperFactory.createObjectMapper();
    }

    @Override
    public void open(MetricConfig config) {
        boolean filterLabelValueCharacters =
                FmqReporterOptions.getBoolean(config, FILTER_LABEL_VALUE_CHARACTER);
        if (!filterLabelValueCharacters) {
            labelValueCharactersFilter = input -> input;
        }
    }

    @Override
    public void close() {
        if (fmqProducer != null) {
            fmqProducer.close();
        }
    }

    @Override
    public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
        Map<String, String> metricTag = new HashMap<>();
        for (final Map.Entry<String, String> dimension : group.getAllVariables().entrySet()) {
            final String key = dimension.getKey();
            final String dimKey =
                    CHARACTER_FILTER.filterCharacters(key.substring(1, key.length() - 1));
            final String dimVal = labelValueCharactersFilter.filterCharacters(dimension.getValue());
            metricTag.put(dimKey, dimVal);
        }
        final String scopedMetricName = getScopedName(metricName, group);
        FmqMetricInfo metricInfo = new FmqMetricInfo(scopedMetricName, metricTag);
        metricInfo.addTag("deploymentId", jobId);
        metricInfo.addTag("deploymentName", jobName);
        if (this.groupingKey != null && this.groupingKey.size() > 0) {
            metricInfo.addTags(this.groupingKey);
        }
        synchronized (this) {
            if (metric instanceof Counter) {
                counterMetrics.put((Counter) metric, metricInfo);
            } else if (metric instanceof Gauge) {
                gaugeMetrics.put((Gauge<?>) metric, metricInfo);
            } else if (metric instanceof Histogram) {
                histogramMetrics.put((Histogram) metric, metricInfo);
            } else if (metric instanceof Meter) {
                meterMetrics.put((Meter) metric, metricInfo);
            } else {
                log.warn(
                        "Cannot add unknown metric type {}. This indicates that the reporter "
                                + "does not support this metric type.",
                        metric.getClass().getName());
            }
        }
    }

    @Override
    public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
        synchronized (this) {
            if (metric instanceof Counter) {
                counterMetrics.remove(metric);
            } else if (metric instanceof Gauge) {
                gaugeMetrics.remove(metric);
            } else if (metric instanceof Histogram) {
                histogramMetrics.remove(metric);
            } else if (metric instanceof Meter) {
                meterMetrics.remove(metric);
            } else {
                log.warn(
                        "Cannot remove unknown metric type {}. This indicates that the reporter "
                                + "does not support this metric type.",
                        metric.getClass().getName());
            }
        }
    }

    private static String getScopedName(String metricName, MetricGroup group) {
        return SCOPE_PREFIX
                + getLogicalScope(group)
                + SCOPE_SEPARATOR
                + CHARACTER_FILTER.filterCharacters(metricName);
    }

    private static String getLogicalScope(MetricGroup group) {
        return LogicalScopeProvider.castFrom(group)
                .getLogicalScope(CHARACTER_FILTER, SCOPE_SEPARATOR);
    }

    @Override
    public void report() {
        long currentTimeMillis = System.currentTimeMillis();
        List<String> sendJSONList = new LinkedList<>();
        sendJSONList.addAll(getGaugesReport(currentTimeMillis));
        sendJSONList.addAll(getCountersReport(currentTimeMillis));
        sendJSONList.addAll(getMetersReport(currentTimeMillis));
        sendJSONList.addAll(getHistogramsReport(currentTimeMillis));
        for (String reportJSON : sendJSONList) {
            ProducerMessage message = new ProducerMessage(reportJSON);
            SendResult fmqSendResult = fmqProducer.sendSync(topic, message);
            if (fmqSendResult.isFailed()) {
                log.error(
                        "flink metric send fmq failed, message:{}, {}", reportJSON, fmqSendResult);
            }
        }
    }

    private List<String> getGaugesReport(long currentTimeMillis) {
        List<String> reportList = new LinkedList<>();
        Map<String, Object> gaugeMap = new HashMap<>(4);
        for (Map.Entry<Gauge<?>, FmqMetricInfo> gauge : gaugeMetrics.entrySet()) {
            double reportValue = 0;
            Gauge<?> gaugeMetric = gauge.getKey();
            final Object value = gaugeMetric.getValue();
            FmqMetricInfo metricInfo = gauge.getValue();
            if (value == null) {
                log.debug("Gauge {} is null-valued, defaulting to 0.", gauge);
            } else if (value instanceof Double) {
                reportValue = (double) value;
            } else if (value instanceof Number) {
                reportValue = ((Number) value).doubleValue();
            } else if (value instanceof Boolean) {
                reportValue = ((Boolean) value) ? 1 : 0;
            }
            log.debug(
                    "Invalid type for Gauge {}: {}, only number types and booleans are supported by this reporter.",
                    gauge,
                    value.getClass().getName());
            gaugeMap.put("name", metricInfo.getName());
            gaugeMap.put("time", currentTimeMillis);
            gaugeMap.put("tags", metricInfo.getTags());
            gaugeMap.put("value", reportValue);
            try {
                reportList.add(jsonMapper.writeValueAsString(gaugeMap));
            } catch (JsonProcessingException e) {
                log.error("flink gauge metric serialize failed", e);
            }
        }
        return reportList;
    }

    private List<String> getCountersReport(long currentTimeMillis) {
        List<String> reportList = new LinkedList<>();
        Map<String, Object> counterMap = new HashMap<>(4);
        for (Map.Entry<Counter, FmqMetricInfo> counter : counterMetrics.entrySet()) {
            Counter counterMetric = counter.getKey();
            final long reportValue = counterMetric.getCount();
            FmqMetricInfo metricInfo = counter.getValue();
            counterMap.put("name", metricInfo.getName());
            counterMap.put("time", currentTimeMillis);
            counterMap.put("tags", metricInfo.getTags());
            counterMap.put("value", reportValue);
            try {
                reportList.add(jsonMapper.writeValueAsString(counterMap));
            } catch (JsonProcessingException e) {
                log.error("flink counter metric serialize failed", e);
            }
        }
        return reportList;
    }

    private List<String> getMetersReport(long currentTimeMillis) {
        List<String> reportList = new LinkedList<>();
        Map<String, Object> counterMap = new HashMap<>(4);
        for (Map.Entry<Meter, FmqMetricInfo> meter : meterMetrics.entrySet()) {
            Meter meterMetric = meter.getKey();
            final double reportValue = meterMetric.getRate();
            FmqMetricInfo metricInfo = meter.getValue();
            counterMap.put("name", metricInfo.getName());
            counterMap.put("time", currentTimeMillis);
            counterMap.put("tags", metricInfo.getTags());
            counterMap.put("value", reportValue);
            try {
                reportList.add(jsonMapper.writeValueAsString(counterMap));
            } catch (JsonProcessingException e) {
                log.error("flink meter metric serialize failed", e);
            }
        }
        return reportList;
    }

    private List<String> getHistogramsReport(long currentTimeMillis) {
        List<String> reportList = new LinkedList<>();
        Map<String, Object> histogramMap = new HashMap<>(4);
        for (Map.Entry<Histogram, FmqMetricInfo> histogram : histogramMetrics.entrySet()) {
            try {
                FmqMetricInfo metricInfo = histogram.getValue();
                Histogram histogramMetric = histogram.getKey();
                histogramMap.put("name", metricInfo.getName() + "_count");
                histogramMap.put("time", currentTimeMillis);
                histogramMap.put("tags", metricInfo.getTags());
                histogramMap.put("value", histogramMetric.getCount());
                reportList.add(jsonMapper.writeValueAsString(histogramMap));

                HistogramStatistics statistics = histogramMetric.getStatistics();
                histogramMap.put("name", metricInfo.getName() + "_min");
                histogramMap.put("value", statistics.getMin());
                reportList.add(jsonMapper.writeValueAsString(histogramMap));

                histogramMap.put("name", metricInfo.getName() + "_max");
                histogramMap.put("value", statistics.getMax());
                reportList.add(jsonMapper.writeValueAsString(histogramMap));

                histogramMap.put("name", metricInfo.getName() + "_avg");
                histogramMap.put("value", statistics.getMean());
                reportList.add(jsonMapper.writeValueAsString(histogramMap));

                for (final Double quantile : HISTORGRAM_QUANTILES) {
                    Map<String, Object> quantileMap = new HashMap<>(4);
                    quantileMap.put("name", metricInfo.getName());
                    quantileMap.put("time", currentTimeMillis);
                    quantileMap.put("tags", metricInfo.addTag("quantile", quantile.toString()));
                    quantileMap.put("value", statistics.getQuantile(quantile));
                    reportList.add(jsonMapper.writeValueAsString(quantileMap));
                }
            } catch (JsonProcessingException e) {
                log.error("flink histogram metric serialize failed", e);
            }
        }
        return reportList;
    }
}
