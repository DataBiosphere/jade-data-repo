package bio.terra.app.controller;

import bio.terra.common.exception.ForbiddenException;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.common.iam.AuthenticatedUserRequestFactory;
import bio.terra.controller.ScheduledApi;
import bio.terra.model.DuosFirecloudGroupsSyncResponse;
import bio.terra.service.duos.DuosService;
import javax.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;

@Controller
public class ScheduledApiController implements ScheduledApi {

  private final HttpServletRequest request;
  private final AuthenticatedUserRequestFactory authenticatedUserRequestFactory;
  private final DuosService duosService;
  private final String tdrServiceAccountEmail;

  public ScheduledApiController(
      HttpServletRequest request,
      AuthenticatedUserRequestFactory authenticatedUserRequestFactory,
      DuosService duosService,
      @Qualifier("tdrServiceAccountEmail") String tdrServiceAccountEmail) {
    this.request = request;
    this.authenticatedUserRequestFactory = authenticatedUserRequestFactory;
    this.duosService = duosService;
    this.tdrServiceAccountEmail = tdrServiceAccountEmail;
  }

  @Override
  public ResponseEntity<DuosFirecloudGroupsSyncResponse> syncDuosDatasetsAuthorizedUsers() {
    String callerEmail = getAuthenticatedInfo().getEmail();
    if (tdrServiceAccountEmail != callerEmail) {
      // TODO maybe introduce a more specific variety of ForbiddenException
      throw new ForbiddenException(
          "Only %s may call this endpoint (called as %s)"
              .formatted(tdrServiceAccountEmail, callerEmail));
    }
    return ResponseEntity.ok(duosService.syncDuosDatasetsAuthorizedUsers());
  }

  private AuthenticatedUserRequest getAuthenticatedInfo() {
    return authenticatedUserRequestFactory.from(request);
  }
}
