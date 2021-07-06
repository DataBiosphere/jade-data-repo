package bio.terra.common;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.support.GeneratedKeyHolder;

public class DaoKeyHolder extends GeneratedKeyHolder {

  public UUID getId() {
    return getField("id", UUID.class).orElse(null);
  }

  public Timestamp getTimestamp(String fieldName) {
    return getField(fieldName, Timestamp.class).orElse(null);
  }

  public Instant getCreatedDate() {
    Timestamp timestamp = getTimestamp("created_date");
    if (timestamp != null) {
      return timestamp.toInstant();
    }
    return null;
  }

  public String getString(String fieldName) {
    return getField(fieldName, String.class).orElse(null);
  }

  public <T> Optional<T> getField(String fieldName, Class<T> type) {
    Map<String, Object> keys = getKeys();
    if (keys != null) {
      Object fieldObject = keys.get(fieldName);
      if (type.isInstance(fieldObject)) {
        return Optional.of(type.cast(fieldObject));
      }
    }
    return Optional.empty();
  }
}
