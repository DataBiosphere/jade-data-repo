package runner.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface SpecificationInterface {
  static final Logger LOG = LoggerFactory.getLogger(SpecificationInterface.class);

  void validate();

  default void display() {
    try {
      // use Jackson to map the specification object to a JSON-formatted text block
      ObjectMapper objectMapper = new ObjectMapper();
      LOG.info(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(this));
    } catch (JsonProcessingException jpEx) {
      throw new RuntimeException(
          "Error converting SpecificationInterface object to a JSON-formatted string");
    }
  }
}
