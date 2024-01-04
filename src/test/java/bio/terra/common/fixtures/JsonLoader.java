package bio.terra.common.fixtures;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.curator.shaded.com.google.common.base.Charsets;
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

  public String loadJson(final String resourcePath) throws IOException {
    try (InputStream stream = classLoader.getResourceAsStream(resourcePath)) {
      return IOUtils.toString(stream, Charsets.UTF_8);
    }
  }

  public <T> T loadObject(final String resourcePath, final Class<T> resourceClass)
      throws IOException {
    final String json = loadJson(resourcePath);
    return objectMapper.readerFor(resourceClass).readValue(json);
  }

  public <T> T loadObject(final String resourcePath, final TypeReference<T> typeReference)
      throws IOException {
    final String json = loadJson(resourcePath);
    return objectMapper.readerFor(typeReference).readValue(json);
  }

  public <T> List<T> loadObjectAsStream(
      final String resourcePath, final TypeReference<T> innerObjectTypeReference)
      throws IOException {
    return Arrays.stream(loadJson(resourcePath).split("\n"))
        .map(json -> this.loadJson(json, innerObjectTypeReference))
        .collect(Collectors.toList());
  }

  public <T> T loadJson(final String json, final TypeReference<T> typeReference) {
    try {
      return objectMapper.readerFor(typeReference).readValue(json);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Error parsing JSON", e);
    }
  }

  public ClassLoader getClassLoader() {
    return classLoader;
  }
}
