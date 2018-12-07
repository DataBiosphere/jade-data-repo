package bio.terra.stairway;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * SafeHashMap wraps a HashMap<String, Object>
 * It provides a subset of the HashMap methods. It localizes code that casts from Object to
 * the target type. It provides a way to set the map to be immutable.
 */
public class SafeHashMap {
    private Map<String, Object> map;

    public SafeHashMap() {
        map = new HashMap<>();
    }

    /**
     * Convert the map to an unmodifiable form.
     */
    public void setImmutable() {
        map = Collections.unmodifiableMap(map);
    }

    /**
     * Return the object from the hash map cast to the right type.
     * Return null, if the Object cannot be cast to that type.
     *
     * @param key - key to lookup in the hash map
     * @param type - class requested
     * @return null if not found or incorrect type; the value cast to the right type otherwise.
     */
    public <T> T get(String key, Class<T> type) {
        // TODO: REVIEWERS this seemed more clever at first than it turns out. It might be better to skip
        // this logic and just do the cast like (T) value. That would throw an exception
        // if the object is not an instance of the type T. It is really less about safety
        // and more about centralizing where we have to override an unchecked cast.
        Object value = map.get(key);
        if (value != null) {
            if (type.isInstance(value)) {
                return type.cast(value);
            }
        }
        return null;
    }

    public void put(String key, Object value) {
        map.put(key, value);
    }

    // TODO: Add other methods as they are needed


    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.JSON_STYLE)
                .append("map", map)
                .toString();
    }
}
