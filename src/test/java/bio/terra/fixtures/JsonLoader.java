package bio.terra.fixtures;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class JsonLoader {
    private ClassLoader classLoader;
    private ObjectMapper objectMapper;

    @Autowired
    public JsonLoader(ObjectMapper objectMapper) {
        this.classLoader = getClass().getClassLoader();
        this.objectMapper = objectMapper;
    }

    public String loadJson(String resourcePath) throws Exception {
        return IOUtils.toString(classLoader.getResourceAsStream(resourcePath));
    }

    public <T> T loadObject(String resourcePath, Class<T> resourceClass) throws Exception {
        String json = loadJson(resourcePath);
        return objectMapper.readerFor(resourceClass).readValue(json);
    }

    public ClassLoader getClassLoader() {
        return classLoader;
    }
}
