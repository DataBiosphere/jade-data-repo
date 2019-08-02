package bio.terra.stairway;

import bio.terra.service.JobMapKeys;
import bio.terra.stairway.exception.DatabaseOperationException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * FlightMap wraps a HashMap<String, Object>
 * It provides a subset of the HashMap methods. It localizes code that casts from Object to
 * the target type. It provides a way to set the map to be immutable.
 */
public class FlightMap {
    private Map<String, Object> map;
    private ObjectMapper objectMapper;

    public FlightMap() {
        map = new HashMap<>();
    }

    public FlightMap(String description, Object request) {
        this();
        map.put(JobMapKeys.DESCRIPTION.getKeyName(), description);
        map.put(JobMapKeys.REQUEST.getKeyName(), request);
    }

    /**
     * Convert the map to an unmodifiable form.
     */
    public void makeImmutable() {
        map = Collections.unmodifiableMap(map);
    }

    /**
     * Return the object from the hash map cast to the right type.
     * Return null, if the Object cannot be cast to that type.
     *
     * @param key - key to lookup in the hash map
     * @param type - class requested
     * @return null if not found
     * @throws ClassCastException if found, not castable to the requested type
     */
    public <T> T get(String key, Class<T> type) {
        Object value = map.get(key);
        if (value == null) {
            return null;
        }

        if (type.isInstance(value)) {
            return type.cast(value);
        }
        throw new ClassCastException("Found value '" + value.toString() +
                "' is not an instance of type " + type.getName());
    }

    public void put(String key, Object value) {
        map.put(key, value);
    }

    public String toJson() {
        try {
            return getObjectMapper().writeValueAsString(map);
        } catch (JsonProcessingException ex) {
            throw new DatabaseOperationException("Failed to convert map to json string", ex);
        }
    }

    public void fromJson(String json) {
        try {
            map = getObjectMapper().readValue(json, new TypeReference<Map<String, Object>>() {});
        } catch (IOException ex) {
            throw new DatabaseOperationException("Failed to convert json string to map", ex);
        }
    }

    /**
     * Build object mapper on use, since we only need it for cases where the map is being read to
     * or written from the database.
     */
    private ObjectMapper getObjectMapper() {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper()
                .registerModule(new ParameterNamesModule())
                .registerModule(new Jdk8Module())
                .registerModule(new JavaTimeModule())
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
        }
        return objectMapper;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.JSON_STYLE)
                .append("map", map)
                .toString();
    }

}
