package bio.terra.app.controller;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.common.iam.AuthenticatedUserRequestFactory;
import bio.terra.controller.ConfigsApi;
import bio.terra.model.ConfigEnableModel;
import bio.terra.model.ConfigGroupModel;
import bio.terra.model.ConfigListModel;
import bio.terra.model.ConfigModel;
import bio.terra.service.auth.iam.IamAction;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.auth.iam.PolicyMemberValidator;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.AssetModelValidator;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.annotations.Api;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.bind.annotation.InitBinder;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;

@Controller
@Api(tags = {"configs"})
public class ConfigsApiController implements ConfigsApi {

  private static final Logger logger = LoggerFactory.getLogger(ConfigsApiController.class);

  private final ObjectMapper objectMapper;
  private final HttpServletRequest request;
  private final IamService iamService;
  private final PolicyMemberValidator policyMemberValidator;
  private final AuthenticatedUserRequestFactory authenticatedUserRequestFactory;
  private final ConfigurationService configurationService;
  private final AssetModelValidator assetModelValidator;
  private final ApplicationConfiguration appConfig;

  @Autowired
  public ConfigsApiController(
      ObjectMapper objectMapper,
      HttpServletRequest request,
      IamService iamService,
      ApplicationConfiguration appConfig,
      PolicyMemberValidator policyMemberValidator,
      AuthenticatedUserRequestFactory authenticatedUserRequestFactory,
      ConfigurationService configurationService,
      AssetModelValidator assetModelValidator) {
    this.objectMapper = objectMapper;
    this.request = request;
    this.iamService = iamService;
    this.appConfig = appConfig;
    this.policyMemberValidator = policyMemberValidator;
    this.authenticatedUserRequestFactory = authenticatedUserRequestFactory;
    this.configurationService = configurationService;
    this.assetModelValidator = assetModelValidator;
  }

  @InitBinder
  protected void initBinder(final WebDataBinder binder) {
    binder.addValidators(policyMemberValidator);
    binder.addValidators(assetModelValidator);
  }

  @Override
  public Optional<ObjectMapper> getObjectMapper() {
    return Optional.ofNullable(objectMapper);
  }

  @Override
  public Optional<HttpServletRequest> getRequest() {
    return Optional.ofNullable(request);
  }

  private AuthenticatedUserRequest getAuthenticatedInfo() {
    return authenticatedUserRequestFactory.from(request);
  }

  @Override
  public ResponseEntity<ConfigModel> getConfig(@PathVariable("name") String name) {
    verifyAuthorization();
    return ResponseEntity.ok(configurationService.getConfig(name));
  }

  @Override
  public ResponseEntity<ConfigListModel> getConfigList() {
    verifyAuthorization();
    ConfigListModel configModelList = configurationService.getConfigList();
    return new ResponseEntity<>(configModelList, HttpStatus.OK);
  }

  @Override
  public ResponseEntity<Void> resetConfig() {
    verifyAuthorization();
    configurationService.reset();
    return new ResponseEntity<>(HttpStatus.NO_CONTENT);
  }

  @Override
  public ResponseEntity<ConfigListModel> setConfigList(
      @Valid @RequestBody ConfigGroupModel configModel) {
    verifyAuthorization();
    ConfigListModel configModelList = configurationService.setConfig(configModel);
    return new ResponseEntity<>(configModelList, HttpStatus.OK);
  }

  @Override
  public ResponseEntity<Void> setFault(
      @PathVariable("name") String name, @Valid @RequestBody ConfigEnableModel configEnable) {
    verifyAuthorization();
    configurationService.setFault(name, configEnable.isEnabled());
    return new ResponseEntity<>(HttpStatus.NO_CONTENT);
  }

  private void verifyAuthorization() {
    iamService.verifyAuthorization(
        getAuthenticatedInfo(),
        IamResourceType.DATAREPO,
        appConfig.getResourceId(),
        IamAction.CONFIGURE);
  }
}
