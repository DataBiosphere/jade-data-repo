package bio.terra.app.controller;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.common.iam.AuthenticatedUserRequestFactory;
import bio.terra.controller.SnapshotAccessRequestApi;
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
@Api(tags = {"SnapshotAccessRequest"})
public class SnapshotAccessRequestApiController implements SnapshotAccessRequestApi {
  private final HttpServletRequest request;
  private final IamService iamService;
  private final SnapshotBuilderService snapshotBuilderService;
  private final AuthenticatedUserRequestFactory authenticatedUserRequestFactory;

  @Autowired
  public SnapshotAccessRequestApiController(
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
  public ResponseEntity<SnapshotAccessRequestResponse> createSnapshotAccessRequest(
      SnapshotAccessRequest snapshotAccessRequest) {
    AuthenticatedUserRequest userRequest = getAuthenticatedInfo();
    iamService.verifyAuthorization(
        userRequest,
        IamResourceType.DATASNAPSHOT,
        snapshotAccessRequest.getSourceSnapshotId().toString(),
        IamAction.CREATE_SNAPSHOT_REQUEST);
    return ResponseEntity.ok(
        snapshotBuilderService.createRequest(userRequest, snapshotAccessRequest));
  }

  @Override
  public ResponseEntity<EnumerateSnapshotAccessRequest> enumerateSnapshotAccessRequests() {
    AuthenticatedUserRequest userRequest = getAuthenticatedInfo();
    Map<UUID, Set<IamRole>> authorizedResources =
        iamService.listAuthorizedResources(userRequest, IamResourceType.SNAPSHOT_BUILDER_REQUEST);
    return ResponseEntity.ok(
        snapshotBuilderService.enumerateRequests(authorizedResources.keySet()));
  }

  @Override
  public ResponseEntity<SnapshotAccessRequestResponse> rejectSnapshotAccessRequest(UUID id) {
    AuthenticatedUserRequest userRequest = getAuthenticatedInfo();
    iamService.verifyAuthorization(
        userRequest, IamResourceType.SNAPSHOT_BUILDER_REQUEST, id.toString(), IamAction.APPROVE);
    return ResponseEntity.ok(snapshotBuilderService.rejectRequest(id));
  }

  private AuthenticatedUserRequest getAuthenticatedInfo() {
    return authenticatedUserRequestFactory.from(request);
  }
}
