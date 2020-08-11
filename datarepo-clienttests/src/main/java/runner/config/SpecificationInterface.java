package runner.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface SpecificationInterface {
  Logger logger = LoggerFactory.getLogger(SpecificationInterface.class);

  int totalNumberToRun = 1;
  int numberToRunInParallel = 1;
  long expectedTimeForEach = 300;
  TimeUnit expectedTimeForEachUnitObj = TimeUnit.SECONDS;
  String description = "";

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
