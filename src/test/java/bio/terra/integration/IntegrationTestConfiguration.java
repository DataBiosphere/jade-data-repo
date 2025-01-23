package bio.terra.integration;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.app.configuration.SamConfiguration;
import bio.terra.common.GcsUtils;
import bio.terra.common.auth.AuthService;
import bio.terra.common.auth.Users;
import bio.terra.common.configuration.TestConfiguration;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.service.auth.iam.sam.SamApiService;
import bio.terra.service.auth.iam.sam.SamIam;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.filedata.google.firestore.EncodeFixture;
import bio.terra.service.resourcemanagement.azure.AzureResourceConfiguration;
import bio.terra.service.resourcemanagement.google.GoogleResourceConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentelemetry.api.OpenTelemetry;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;

@Configuration
@Import({
  TestConfiguration.class,
  AuthService.class,
  ObjectMapper.class,
  JsonLoader.class,
  DataRepoClient.class,
  SamFixtures.class,
  DataRepoFixtures.class,
  Users.class,
  GcsUtils.class,
  EncodeFixture.class,
  // These are required to support AuthService.makePetAccountToken()
  SamIam.class,
  SamApiService.class,
  ConfigurationService.class,
  SamApiService.class
})
@EnableConfigurationProperties({
  SamConfiguration.class,
  GoogleResourceConfiguration.class,
  ApplicationConfiguration.class,
  AzureResourceConfiguration.class
})
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Profile("integrationtest")
public class IntegrationTestConfiguration {

  @Bean("tdrServiceAccountEmail")
  public String tdrServiceAccountEmail() {
    // Provide a default value for the service account email when running a spring-context aware
    // test to avoid having to set it in the test environment.
    return "";
  }

  @Bean
  public OpenTelemetry openTelemetry() {
    return OpenTelemetry.noop();
  }
}
