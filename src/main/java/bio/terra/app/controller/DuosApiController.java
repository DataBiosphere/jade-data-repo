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
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;

@Controller
public class DuosApiController implements DuosApi {

  private final HttpServletRequest request;
  private final AuthenticatedUserRequestFactory authenticatedUserRequestFactory;
  private final ApplicationConfiguration appConfig;
  private final IamService iamService;
  private final DuosService duosService;

  public DuosApiController(
      HttpServletRequest request,
      AuthenticatedUserRequestFactory authenticatedUserRequestFactory,
      ApplicationConfiguration appConfig,
      IamService iamService,
      DuosService duosService) {
    this.request = request;
    this.authenticatedUserRequestFactory = authenticatedUserRequestFactory;
    this.appConfig = appConfig;
    this.iamService = iamService;
    this.duosService = duosService;
  }

  @Override
  public ResponseEntity<List<DuosFirecloudGroupModel>> retrieveDuosFirecloudGroups() {
    return ResponseEntity.ok(duosService.retrieveFirecloudGroups());
  }

  @Override
  public ResponseEntity<DuosFirecloudGroupModel> retrieveDuosFirecloudGroup(String duosId) {
    return ResponseEntity.ok(duosService.retrieveFirecloudGroup(duosId));
  }

  @Override
  public ResponseEntity<DuosFirecloudGroupModel> syncDuosDatasetAuthorizedUsers(String duosId) {
    iamService.verifyAuthorization(
        getAuthenticatedInfo(),
        IamResourceType.DATAREPO,
        appConfig.getResourceId(),
        IamAction.SYNC_DUOS_USERS);
    return ResponseEntity.ok(duosService.syncDuosDatasetAuthorizedUsers(duosId));
  }

  private AuthenticatedUserRequest getAuthenticatedInfo() {
    return authenticatedUserRequestFactory.from(request);
  }
}