package bio.terra.dao;

import org.springframework.jdbc.support.GeneratedKeyHolder;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public class DaoKeyHolder extends GeneratedKeyHolder {

    public UUID getId() {
        Map<String, Object> keys = getKeys();
        if (keys != null) {
            Object id = keys.get("id");
            if (id != null) {
                return (UUID)id;
            }
        }
        return null;
    }

    public Instant getCreatedDate() {
        Map<String, Object> keys = getKeys();
        if (keys != null) {
            Object createdDate = keys.get("created_date");
            if (createdDate != null) {
                Timestamp timestamp = (Timestamp)createdDate;
                return timestamp.toInstant();
            }
        }
        return null;
    }

}
