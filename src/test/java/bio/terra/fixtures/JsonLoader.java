package bio.terra.fixtures;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class JsonLoader {
    private ClassLoader classLoader;
    private ObjectMapper objectMapper;

    @Autowired
    public JsonLoader(ObjectMapper objectMapper) {
        this.classLoader = getClass().getClassLoader();
        this.objectMapper = objectMapper;
    }

    public String loadJson(String resourcePath) throws IOException {
        return IOUtils.toString(classLoader.getResourceAsStream(resourcePath));
    }

    public <T> T loadObject(String resourcePath, Class<T> resourceClass) throws IOException {
        String json = loadJson(resourcePath);
        return objectMapper.readerFor(resourceClass).readValue(json);
    }

    public ClassLoader getClassLoader() {
        return classLoader;
    }
}
