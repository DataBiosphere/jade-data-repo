package bio.terra.app.usermetrics;

import java.util.HashMap;
import java.util.Map;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;

/**
 * This class wraps a ThreadLocal variable to store API request properties to log (e.g. method,
 * path, billing profile id).
 *
 * <p>Only one instance of the ThreadLocal is created per thread, so properties added in a single
 * API request will get grouped together even if they are set in different methods.
 */
@Component
@RequestScope
public class UserLoggingMetrics {

  private final Map<String, Object> metrics = new HashMap<>();

  /**
   * Get the current thread's metrics instance. If no metrics have been set, return the default
   * empty HashMap.
   *
   * @return Map<String, Object> metrics
   */
  public Map<String, Object> get() {
    return metrics;
  }

  /**
   * Add a new value to the metrics map. If the map already contains a value for this key it will be
   * replaced.
   */
  public void set(String key, Object value) {
    metrics.put(key, value);
  }

  /**
   * Add multiple values to the current thread's metrics map. Any existing values with the same key
   * will get replaced.
   *
   * @param value Map of metrics to add
   */
  public void setAll(Map<String, Object> value) {
    metrics.putAll(value);
  }
}
