package bio.terra.app.usermetrics;

import static org.springframework.web.context.WebApplicationContext.SCOPE_REQUEST;

import java.util.HashMap;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.stereotype.Component;

/**
 * This class wraps a ThreadLocal variable to store API request properties to log (e.g. method,
 * path, billing profile id).
 *
 * <p>Only one instance of the ThreadLocal is created per thread, so properties added in a single
 * API request will get grouped together even if they are set in different methods.
 */
@Component
@Scope(value = SCOPE_REQUEST, proxyMode = ScopedProxyMode.TARGET_CLASS)
public class UserLoggingMetrics {

  private final HashMap<String, Object> metrics = new HashMap<>();

  /**
   * Get the current thread's metrics instance. If no metrics have been set, return the default
   * empty HashMap.
   *
   * @return HashMap<String, Object> metrics
   */
  public HashMap<String, Object> get() {
    return metrics;
  }

  /**
   * Add a new value to the current thread's metrics map. If the map already contains a value for
   * this key it will be replaced.
   */
  public void set(String key, Object value) {
    metrics.put(key, value);
  }

  /**
   * Add multiple values to the current thread's metrics map. Any existing values with the same key
   * will get replaced.
   *
   * @param value HashMap of metrics to add
   */
  public void setAll(HashMap<String, Object> value) {
    metrics.putAll(value);
  }
}
