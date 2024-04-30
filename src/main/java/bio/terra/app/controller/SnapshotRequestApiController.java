package bio.terra.app.controller;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.common.iam.AuthenticatedUserRequestFactory;
import bio.terra.controller.SnapshotRequestApi;
import bio.terra.model.EnumerateSnapshotAccessRequest;
import bio.terra.model.SnapshotAccessRequest;
import bio.terra.model.SnapshotAccessRequestResponse;
import bio.terra.service.auth.iam.IamAction;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.snapshotbuilder.SnapshotBuilderService;
import io.swagger.annotations.Api;
import jakarta.servlet.http.HttpServletRequest;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;

@Controller
@Api(tags = {"SnapshotRequest"})
public class SnapshotRequestApiController implements SnapshotRequestApi {
  private final HttpServletRequest request;
  private final IamService iamService;
  private final SnapshotBuilderService snapshotBuilderService;
  private final AuthenticatedUserRequestFactory authenticatedUserRequestFactory;

  @Autowired
  public SnapshotRequestApiController(
      HttpServletRequest request,
      IamService iamService,
      SnapshotBuilderService snapshotBuilderService,
      AuthenticatedUserRequestFactory authenticatedUserRequestFactory) {
    this.request = request;
    this.iamService = iamService;
    this.snapshotBuilderService = snapshotBuilderService;
    this.authenticatedUserRequestFactory = authenticatedUserRequestFactory;
  }

  @Override
  public ResponseEntity<SnapshotAccessRequestResponse> createSnapshotRequest(
      SnapshotAccessRequest snapshotAccessRequest) {
    AuthenticatedUserRequest userRequest = getAuthenticatedInfo();
    iamService.verifyAuthorization(
        userRequest,
        IamResourceType.DATASNAPSHOT,
        snapshotAccessRequest.getSourceSnapshotId().toString(),
        IamAction.CREATE_SNAPSHOT_REQUEST);
    return ResponseEntity.ok(
        snapshotBuilderService.createSnapshotRequest(userRequest, snapshotAccessRequest));
  }

  @Override
  public ResponseEntity<EnumerateSnapshotAccessRequest> enumerateSnapshotRequests() {
    AuthenticatedUserRequest userRequest = getAuthenticatedInfo();
    Map<UUID, Set<IamRole>> authorizedResources =
        iamService.listAuthorizedResources(userRequest, IamResourceType.SNAPSHOT_BUILDER_REQUEST);
    return ResponseEntity.ok(
        snapshotBuilderService.enumerateSnapshotRequests(authorizedResources.keySet()));
  }

  private AuthenticatedUserRequest getAuthenticatedInfo() {
    return authenticatedUserRequestFactory.from(request);
  }
}
