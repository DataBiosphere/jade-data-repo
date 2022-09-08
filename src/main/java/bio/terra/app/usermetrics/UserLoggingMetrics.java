package bio.terra.app.usermetrics;

import java.util.HashMap;
import org.springframework.stereotype.Component;

/**
 * This class wraps a ThreadLocal variable to store API request properties to log (e.g. method,
 * path, billing profile id).
 *
 * <p>Only one instance of the ThreadLocal is created per thread, so properties added in a single
 * API request will get grouped together even if they are set in different methods.
 */
@Component
public class UserLoggingMetrics {

  private static final ThreadLocal<HashMap<String, Object>> metrics =
      ThreadLocal.withInitial(() -> new HashMap<>());

  /**
   * Get the current thread's metrics instance. If no metrics have been set, return the default
   * empty HashMap.
   *
   * @return HashMap<String, Object> metrics
   */
  public HashMap<String, Object> get() {
    return metrics.get();
  }

  /**
   * Add a new value to the current thread's metrics map. If the map already contains a value for
   * this key it will be replaced.
   */
  public void set(String key, Object value) {
    metrics.get().put(key, value);
  }

  /**
   * Add multiple values to the current thread's metrics map. Any existing values with the same key
   * will get replaced.
   *
   * @param value HashMap of metrics to add
   */
  public void setAll(HashMap<String, Object> value) {
    HashMap<String, Object> properties = metrics.get();
    properties.putAll(value);
    metrics.set(properties);
  }
}
