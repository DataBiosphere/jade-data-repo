package bio.terra.app.controller;

import bio.terra.app.configuration.OauthConfiguration;
import bio.terra.app.configuration.OpenIDConnectConfiguration;
import bio.terra.app.configuration.SamConfiguration;
import bio.terra.app.configuration.TerraConfiguration;
import bio.terra.controller.UnauthenticatedApi;
import bio.terra.model.RepositoryConfigurationModel;
import bio.terra.model.RepositoryStatusModel;
import bio.terra.service.job.JobService;
import bio.terra.service.status.StatusService;
import io.swagger.annotations.Api;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@Api(tags = {"unauthenticated"})
public class UnauthenticatedApiController implements UnauthenticatedApi {

  private final OauthConfiguration oauthConfig;
  private final OpenIDConnectConfiguration openIDConnectConfiguration;
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

  public UnauthenticatedApiController(
      OauthConfiguration oauthConfig,
      OpenIDConnectConfiguration openIDConnectConfiguration,
      JobService jobService,
      Environment env,
      StatusService statusService,
      TerraConfiguration terraConfiguration,
      SamConfiguration samConfiguration) {
    this.oauthConfig = oauthConfig;
    this.openIDConnectConfiguration = openIDConnectConfiguration;
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
  public ResponseEntity<RepositoryStatusModel> serviceStatus() {
    RepositoryStatusModel repoStatus = statusService.getStatus();
    HttpStatus httpStatus = repoStatus.isOk() ? HttpStatus.OK : HttpStatus.SERVICE_UNAVAILABLE;
    return new ResponseEntity<>(repoStatus, httpStatus);
  }

  @Override
  public ResponseEntity<RepositoryConfigurationModel> retrieveRepositoryConfig() {
    RepositoryConfigurationModel configurationModel =
        new RepositoryConfigurationModel()
            .clientId(oauthConfig.clientId())
            .oidcClientId(openIDConnectConfiguration.getClientId())
            .activeProfiles(Arrays.asList(env.getActiveProfiles()))
            .semVer(semVer)
            .gitHash(gitHash)
            .terraUrl(terraConfiguration.getBasePath())
            .samUrl(samConfiguration.getBasePath())
            .authorityEndpoint(openIDConnectConfiguration.getAuthorityEndpoint());

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
    return "redirect:swagger-ui.html";
  }

  @RequestMapping(value = "/swagger-ui.html")
  public String getSwaggerUI(Model model) {
    model.addAttribute("oauthClientId", oauthConfig.clientId());
    model.addAttribute("oidcClientId", openIDConnectConfiguration.getClientId());
    return "index";
  }
}
