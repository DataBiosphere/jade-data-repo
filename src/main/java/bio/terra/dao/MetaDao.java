package bio.terra.dao;

import org.springframework.jdbc.support.KeyHolder;

import java.util.Map;
import java.util.UUID;

public abstract class MetaDao<T> {
//    abstract UUID create(T object);
//    T retrieve(String id);
    //void update(T object);
//    void delete(String id);
//    List<T> enumerate();

    //TODO: find a better home for this, temporary fix for findBugs
    UUID getIdKey(KeyHolder keyHolder) {
        Map<String, Object> keys = keyHolder.getKeys();
        if (keys != null) {
            Object id = keys.get("id");
            if (id != null) {
                return (UUID)id;
            }
        }
        return null;
    }

}
