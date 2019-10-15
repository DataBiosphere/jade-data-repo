package bio.terra.app.controller;

import bio.terra.app.configuration.OauthConfiguration;
import bio.terra.controller.UnauthenticatedApi;
import bio.terra.model.RepositoryConfigurationModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import javax.servlet.http.HttpServletRequest;
import java.util.Arrays;
import java.util.Optional;

@Controller
public class UnauthenticatedApiController implements UnauthenticatedApi {

    private final ObjectMapper objectMapper;

    private final HttpServletRequest request;

    private final OauthConfiguration oauthConfig;

    @Autowired
    private Environment env;

    @Autowired
    public UnauthenticatedApiController(
        ObjectMapper objectMapper,
        HttpServletRequest request,
        OauthConfiguration oauthConfig
    ) {
        this.objectMapper = objectMapper;
        this.request = request;
        this.oauthConfig = oauthConfig;
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
        return new ResponseEntity<>(HttpStatus.OK);
    }

    @Override
    public ResponseEntity<RepositoryConfigurationModel> retrieveRepositoryConfig() {
        RepositoryConfigurationModel configurationModel = new RepositoryConfigurationModel()
            .clientId(oauthConfig.getClientId())
            .activeProfiles(Arrays.asList(env.getActiveProfiles()));
        return new ResponseEntity<>(configurationModel, HttpStatus.OK);
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
