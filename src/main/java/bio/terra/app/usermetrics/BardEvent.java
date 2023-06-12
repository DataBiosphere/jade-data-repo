package bio.terra.app.usermetrics;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang3.builder.ToStringBuilder;

/** Payload that gets sent to bard to log a user events */
public class BardEvent {
  private static final String APP_ID_FIELD = "appId";
  private static final String HOSTNAME_FIELD = "hostname";
  private static final String USE_BIGQUERY_FIELD = "useBigQuery";
  private final String event;
  private final Map<String, Object> properties;

  public BardEvent(String event, Map<String, Object> properties, String appId, String hostName) {
    this.event = event;
    this.properties = new HashMap<>();
    this.properties.putAll(properties);
    this.properties.put(APP_ID_FIELD, appId);
    this.properties.put(HOSTNAME_FIELD, hostName);
    this.properties.put(USE_BIGQUERY_FIELD, true);
  }

  public String getEvent() {
    return event;
  }

  public Map<String, Object> getProperties() {
    return properties;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BardEvent bardEvent = (BardEvent) o;
    return Objects.equals(event, bardEvent.event)
        && Objects.equals(properties, bardEvent.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(event, properties);
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("event", event)
        .append("properties", properties)
        .toString();
  }
}
