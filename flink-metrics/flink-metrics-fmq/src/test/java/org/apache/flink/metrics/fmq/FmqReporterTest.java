/*
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

package org.apache.flink.metrics.fmq;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.util.TestHistogram;
import org.apache.flink.metrics.util.TestMeter;

import com.mashape.unirest.http.exceptions.UnirestException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** Basic test for {@link FmqReporter}. */
class FmqReporterTest {
    private static final String[] LABEL_NAMES = {"label1", "label2"};
    private static final String[] LABEL_VALUES = new String[] {"value1", "value2"};
    private static final String LOGICAL_SCOPE = "fmq_scope";
    private static final String SCOPE_PREFIX = "flink_" + LOGICAL_SCOPE + "_";
    private static final String BOOTSTRAP_SERVERS =
            "10.110.5.44:9092,10.110.5.241:9092,10.110.5.242:9092";
    private static final String TOPIC = "rdc_bigd_fide_alert_local";
    private static final String CURRENT_SITE = "local";
    private static final String JOB_ID = "testJobId_1234567890";
    private static final String JOB_NAME = "testJobName";
    private MetricGroup metricGroup;
    private FmqReporter reporter;

    @BeforeEach
    void setupReporter() {
        reporter =
                new FmqReporter(
                        BOOTSTRAP_SERVERS,
                        TOPIC,
                        CURRENT_SITE,
                        JOB_ID,
                        JOB_NAME,
                        Collections.emptyMap());

        metricGroup =
                TestUtils.createTestMetricGroup(
                        LOGICAL_SCOPE, TestUtils.toMap(LABEL_NAMES, LABEL_VALUES));
    }

    @AfterEach
    void teardown() throws Exception {
        if (reporter != null) {
            reporter.close();
        }
    }

    @Test
    void counterIsReportedAsPrometheusGauge() throws UnirestException {
        Counter testCounter = new SimpleCounter();
        testCounter.inc(7);

        assertThatGaugeIsExported(testCounter, "testCounter", "7.0");
    }

    @Test
    void gaugeIsReportedAsPrometheusGauge() throws UnirestException {
        Gauge<Integer> testGauge = () -> 1;

        assertThatGaugeIsExported(testGauge, "testGauge", "1.0");
    }

    @Test
    void nullGaugeDoesNotBreakReporter() throws UnirestException {
        Gauge<Integer> testGauge = () -> null;

        assertThatGaugeIsExported(testGauge, "testGauge", "0.0");
    }

    @Test
    void meterRateIsReportedAsPrometheusGauge() throws UnirestException {
        Meter testMeter = new TestMeter();

        assertThatGaugeIsExported(testMeter, "testMeter", "5.0");
    }

    private void assertThatGaugeIsExported(Metric metric, String name, String expectedValue)
            throws UnirestException {
        addMetricAndReport(metric, name);
    }

    @Test
    void histogramIsReportedAsPrometheusSummary() throws UnirestException {
        Histogram testHistogram = new TestHistogram();

        String histogramName = "testHistogram";
        String summaryName = SCOPE_PREFIX + histogramName;

        addMetricAndReport(testHistogram, histogramName);
    }

    /**
     * Metrics with the same name are stored by the reporter in a shared data-structure. This test
     * ensures that a metric is unregistered from Prometheus even if other metrics with the same
     * name still exist.
     */
    @Test
    void metricIsRemovedWhileOtherMetricsWithSameNameExist() throws UnirestException {
        String metricName = "metric";

        Counter metric1 = new SimpleCounter();
        Counter metric2 = new SimpleCounter();

        final Map<String, String> variables2 = new HashMap<>(metricGroup.getAllVariables());
        final Map.Entry<String, String> entryToModify = variables2.entrySet().iterator().next();
        variables2.put(entryToModify.getKey(), "some_value");
        final MetricGroup metricGroup2 = TestUtils.createTestMetricGroup(LOGICAL_SCOPE, variables2);

        reporter.notifyOfAddedMetric(metric1, metricName, metricGroup);
        reporter.notifyOfAddedMetric(metric2, metricName, metricGroup2);
        reporter.notifyOfRemovedMetric(metric1, metricName, metricGroup);

        reporter.report();
    }

    @Test
    void registeringSameMetricTwiceDoesNotThrowException() {
        Counter counter = new SimpleCounter();
        counter.inc();
        String counterName = "testCounter";

        reporter.notifyOfAddedMetric(counter, counterName, metricGroup);
        reporter.notifyOfAddedMetric(counter, counterName, metricGroup);
    }

    private void addMetricAndReport(Metric metric, String metricName) {
        reporter.notifyOfAddedMetric(metric, metricName, metricGroup);
        reporter.report();
    }
}
