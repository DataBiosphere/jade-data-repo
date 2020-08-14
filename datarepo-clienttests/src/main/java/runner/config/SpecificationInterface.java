package runner.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface SpecificationInterface {
  Logger logger = LoggerFactory.getLogger(SpecificationInterface.class);

  void validate();

  default String display() {
    try {
      // use Jackson to map the specification object to a JSON-formatted text block
      ObjectMapper objectMapper = new ObjectMapper();
      return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(this);
    } catch (JsonProcessingException jpEx) {
      logger.error(
          "Error converting SpecificationInterface object to a JSON-formatted string: {}",
          this.toString(),
          jpEx);
      throw new RuntimeException(
          "Error converting SpecificationInterface object to a JSON-formatted string", jpEx);
    }
  }
}
