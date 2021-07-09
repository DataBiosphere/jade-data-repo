package bio.terra.datarepo.app.controller;

import bio.terra.datarepo.api.UnauthenticatedApi;
import bio.terra.datarepo.app.configuration.OauthConfiguration;
import bio.terra.datarepo.app.configuration.SamConfiguration;
import bio.terra.datarepo.app.configuration.TerraConfiguration;
import bio.terra.datarepo.model.RepositoryConfigurationModel;
import bio.terra.datarepo.model.RepositoryStatusModel;
import bio.terra.datarepo.service.configuration.StatusService;
import bio.terra.datarepo.service.job.JobService;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.annotations.Api;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;
import javax.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@Api(tags = {"unauthenticated"})
public class UnauthenticatedApiController implements UnauthenticatedApi {

  private final ObjectMapper objectMapper;
  private final HttpServletRequest request;
  private final OauthConfiguration oauthConfig;
  private final Logger logger = LoggerFactory.getLogger(UnauthenticatedApiController.class);
  private final JobService jobService;
  private final Environment env;
  private final StatusService statusService;
  private final SamConfiguration samConfiguration;
  private final TerraConfiguration terraConfiguration;

  private static final String DEFAULT_SEMVER = "1.0.0-UNKNOWN";
  private static final String DEFAULT_GITHASH = "00000000";

  private final String semVer;
  private final String gitHash;

  @Autowired
  public UnauthenticatedApiController(
      ObjectMapper objectMapper,
      HttpServletRequest request,
      OauthConfiguration oauthConfig,
      JobService jobService,
      Environment env,
      StatusService statusService,
      TerraConfiguration terraConfiguration,
      SamConfiguration samConfiguration) {
    this.objectMapper = objectMapper;
    this.request = request;
    this.oauthConfig = oauthConfig;
    this.jobService = jobService;
    this.env = env;
    this.statusService = statusService;
    this.terraConfiguration = terraConfiguration;
    this.samConfiguration = samConfiguration;

    Properties properties = new Properties();
    try (InputStream versionFile =
        getClass().getClassLoader().getResourceAsStream("version.properties")) {
      properties.load(versionFile);
    } catch (IOException e) {
      logger.warn("Could not access version.properties file, using defaults");
    }
    semVer = Optional.ofNullable(properties.getProperty("semVer")).orElse(DEFAULT_SEMVER);
    gitHash = Optional.ofNullable(properties.getProperty("gitHash")).orElse(DEFAULT_GITHASH);
  }

  @Override
  public Optional<ObjectMapper> getObjectMapper() {
    return Optional.ofNullable(objectMapper);
  }

  @Override
  public Optional<HttpServletRequest> getRequest() {
    return Optional.ofNullable(request);
  }

  @Override
  public ResponseEntity<RepositoryStatusModel> serviceStatus() {
    RepositoryStatusModel repoStatus = statusService.getStatus();
    HttpStatus httpStatus = repoStatus.isOk() ? HttpStatus.OK : HttpStatus.SERVICE_UNAVAILABLE;
    return new ResponseEntity<>(repoStatus, httpStatus);
  }

  @Override
  public ResponseEntity<RepositoryConfigurationModel> retrieveRepositoryConfig() {
    RepositoryConfigurationModel configurationModel =
        new RepositoryConfigurationModel()
            .clientId(oauthConfig.getClientId())
            .activeProfiles(Arrays.asList(env.getActiveProfiles()))
            .semVer(semVer)
            .gitHash(gitHash)
            .terraUrl(terraConfiguration.getBasePath())
            .samUrl(samConfiguration.getBasePath());

    return new ResponseEntity<>(configurationModel, HttpStatus.OK);
  }

  @Override
  public ResponseEntity<Void> shutdownRequest() {
    try {
      if (!jobService.shutdown()) {
        // Shutdown did not complete. Return an error so the caller knows that
        return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
      }
    } catch (InterruptedException ex) {
      return new ResponseEntity<>(HttpStatus.SERVICE_UNAVAILABLE);
    }
    return new ResponseEntity<>(HttpStatus.NO_CONTENT);
  }

  /** Home redirection to swagger api documentation */
  @RequestMapping(value = "/")
  public String index() {
    System.out.println("swagger-ui.html");
    return "redirect:swagger-ui.html";
  }
}
