package bio.terra.app.controller;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.common.iam.AuthenticatedUserRequestFactory;
import bio.terra.controller.DuosApi;
import bio.terra.model.DuosFirecloudGroupModel;
import bio.terra.service.auth.iam.IamAction;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.duos.DuosService;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.annotations.Api;
import java.util.Optional;
import javax.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;

@Controller
@Api(tags = {"duos"})
public class DuosApiController implements DuosApi {

  private final Logger logger = LoggerFactory.getLogger(DuosApiController.class);

  private final HttpServletRequest request;
  private final ObjectMapper objectMapper;
  private final AuthenticatedUserRequestFactory authenticatedUserRequestFactory;
  private final ApplicationConfiguration appConfig;
  private final IamService iamService;
  private final DuosService duosService;

  public DuosApiController(
      HttpServletRequest request,
      ObjectMapper objectMapper,
      AuthenticatedUserRequestFactory authenticatedUserRequestFactory,
      ApplicationConfiguration appConfig,
      IamService iamService,
      DuosService duosService) {
    this.request = request;
    this.objectMapper = objectMapper;
    this.authenticatedUserRequestFactory = authenticatedUserRequestFactory;
    this.appConfig = appConfig;
    this.iamService = iamService;
    this.duosService = duosService;
  }

  @Override
  public ResponseEntity<DuosFirecloudGroupModel> syncDuosDatasetAuthorizedUsers(String duosId) {
    // TODO: at present, only TDR Admins hold the "configure" action on the "datarepo" resource.
    // This check effectively limits syncing to TDR Admins.
    // But rather than overloading an existing action, a more targeted approach would be to create
    // new action(s) / role(s) in Sam to gate this operation.
    iamService.verifyAuthorization(
        getAuthenticatedInfo(),
        IamResourceType.DATAREPO,
        appConfig.getResourceId(),
        IamAction.CONFIGURE);
    return ResponseEntity.ok(duosService.syncDuosDatasetAuthorizedUsers(duosId));
  }

  private AuthenticatedUserRequest getAuthenticatedInfo() {
    return authenticatedUserRequestFactory.from(request);
  }

  @Override
  public Optional<HttpServletRequest> getRequest() {
    return Optional.ofNullable(request);
  }

  @Override
  public Optional<ObjectMapper> getObjectMapper() {
    return Optional.ofNullable(objectMapper);
  }
}
