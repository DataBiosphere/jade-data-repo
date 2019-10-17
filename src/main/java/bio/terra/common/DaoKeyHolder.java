package bio.terra.common;

import org.springframework.jdbc.support.GeneratedKeyHolder;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public class DaoKeyHolder extends GeneratedKeyHolder {

    public UUID getId() {
        return getField("id", UUID.class);
    }

    public Timestamp getTimestamp(String fieldName) {
        return getField(fieldName, Timestamp.class);
    }

    public Instant getCreatedDate() {
        Timestamp timestamp = getTimestamp("created_date");
        if (timestamp != null) {
            return timestamp.toInstant();
        }
        return null;
    }

    public <T> T getField(String fieldName, Class<T> type) {
        Map<String, Object> keys = getKeys();
        if (keys != null) {
            Object fieldObject = keys.get(fieldName);
            if (type.isInstance(fieldObject)) {
                return type.cast(fieldObject);
            }
        }
        return null;
    }

}
