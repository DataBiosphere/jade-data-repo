package bio.terra.app.controller;

import bio.terra.app.configuration.OauthConfiguration;
import bio.terra.controller.UnauthenticatedApi;
import bio.terra.model.RepositoryConfigurationModel;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.job.JobService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;

@Controller
public class UnauthenticatedApiController implements UnauthenticatedApi {

    private final ObjectMapper objectMapper;
    private final HttpServletRequest request;
    private final OauthConfiguration oauthConfig;
    private final Logger logger = LoggerFactory.getLogger(UnauthenticatedApiController.class);
    private final JobService jobService;
    private final Environment env;
    private final ConfigurationService configurationService;

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
        ConfigurationService configurationService
    ) {
        this.objectMapper = objectMapper;
        this.request = request;
        this.oauthConfig = oauthConfig;
        this.jobService = jobService;
        this.env = env;
        this.configurationService = configurationService;

        Properties properties = new Properties();
        try (InputStream versionFile = getClass().getClassLoader().getResourceAsStream("version.properties")) {
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
    public ResponseEntity<Void> serviceStatus() {
        if (configurationService.testInsertFault(ConfigEnum.LIVENESS_FAULT)) {
            logger.info("LIVENESS_FAULT insertion - failing status response");
            return new ResponseEntity<>(HttpStatus.SERVICE_UNAVAILABLE);
        }
        return new ResponseEntity<>(HttpStatus.OK);
    }

    @Override
    public ResponseEntity<RepositoryConfigurationModel> retrieveRepositoryConfig() {
        RepositoryConfigurationModel configurationModel = new RepositoryConfigurationModel()
            .clientId(oauthConfig.getClientId())
            .activeProfiles(Arrays.asList(env.getActiveProfiles()))
            .semVer(semVer)
            .gitHash(gitHash);

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

    /**
     * Home redirection to swagger api documentation
     */
    @RequestMapping(value = "/")
    public String index() {
        System.out.println("swagger-ui.html");
        return "redirect:swagger-ui.html";
    }

}
