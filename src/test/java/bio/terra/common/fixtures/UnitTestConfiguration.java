package bio.terra.common.fixtures;

import bio.terra.app.configuration.ApplicationConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Profile("unittest")
@Configuration
public class UnitTestConfiguration {

  private final ObjectMapper objectMapper = new ApplicationConfiguration().objectMapper();

  @Bean("tdrServiceAccountEmail")
  public String tdrServiceAccountEmail() {
    // Provide a default value for the service account email when running a spring-context aware
    // unit test to avoid having to set it in the test environment.
    return "";
  }

  @Bean("objectMapper")
  public ObjectMapper objectMapper() {
    // Provide the same objectmapper that the application configuration provides. Without this,
    // the default Jackson objectmapper is used, which is configured differently.
    return objectMapper;
  }
}
