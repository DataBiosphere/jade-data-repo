package runner.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public interface SpecificationInterface {
  void validate();

  default String display() {
    try {
      // use Jackson to map the specification object to a JSON-formatted text block
      ObjectMapper objectMapper = new ObjectMapper();
      return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(this);
    } catch (JsonProcessingException jpEx) {
      throw new RuntimeException(
          "Error converting SpecificationInterface object to a JSON-formatted string");
    }
  }
}
