package bio.terra.app.usermetrics;

import java.util.HashMap;
import org.springframework.stereotype.Component;

@Component
public class UserLoggingMetrics {

  private static final ThreadLocal<HashMap<String, Object>> metrics =
      ThreadLocal.withInitial(() -> new HashMap<>());

  public HashMap<String, Object> get() {
    return metrics.get();
  }

  public void set(String key, Object value) {
    metrics.get().put(key, value);
  }

  public void setAll(HashMap<String, Object> value) {
    HashMap<String, Object> properties = metrics.get();
    properties.putAll(value);
    metrics.set(properties);
  }
}
