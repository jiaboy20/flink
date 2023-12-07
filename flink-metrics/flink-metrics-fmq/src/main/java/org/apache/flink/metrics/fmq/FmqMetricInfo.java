package org.apache.flink.metrics.fmq;

import java.util.HashMap;
import java.util.Map;

final class FmqMetricInfo {
  // measurement name
  private String name;

  // tags for metric
  private Map<String, String> tags = new HashMap<>();

  public FmqMetricInfo(String name, Map<String, String> tags) {
    this.name = name;
    this.tags = tags;
  }

  public String getName() {
    return name;
  }

  public Map<String, String> getTags() {
    return tags;
  }

  public Map<String, String> addTag(String key, String val) {
    if (key == null || "".equals(key)) return this.tags;
    if (val == null || "".equals(val)) return this.tags;
    this.tags.put(key, val);
    return this.tags;
  }

  public Map<String, String> addTags(Map<String, String> newTags) {
    if (newTags == null || newTags.size() == 0) return this.tags;
    tags.putAll(newTags);
    return this.tags;
  }

  @Override
  public String toString() {
    return "FmqMetricInfo{" +
      "name='" + name + '\'' +
      ", tags=" + tags +
      '}';
  }
}
