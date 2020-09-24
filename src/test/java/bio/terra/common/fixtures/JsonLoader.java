package bio.terra.common.fixtures;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;

@Component
public class JsonLoader {
    private ClassLoader classLoader;
    private ObjectMapper objectMapper;

    @Autowired
    public JsonLoader(ObjectMapper objectMapper) {
        this.classLoader = getClass().getClassLoader();
        this.objectMapper = objectMapper;
    }

    public String loadJson(final String resourcePath) throws IOException {
        try (InputStream stream = classLoader.getResourceAsStream(resourcePath)) {
            return IOUtils.toString(stream);
        }
    }

    public <T> T loadObject(final String resourcePath, final Class<T> resourceClass) throws IOException {
        final String json = loadJson(resourcePath);
        return objectMapper.readerFor(resourceClass).readValue(json);
    }

    public ClassLoader getClassLoader() {
        return classLoader;
    }
}
