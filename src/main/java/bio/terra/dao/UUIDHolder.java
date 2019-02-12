package bio.terra.dao;

import org.springframework.jdbc.support.GeneratedKeyHolder;

import java.util.Map;
import java.util.UUID;

public class UUIDHolder extends GeneratedKeyHolder {

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

}
