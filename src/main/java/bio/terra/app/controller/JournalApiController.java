package bio.terra.app.controller;

import bio.terra.common.IamResourceTypeCODEC;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.common.iam.AuthenticatedUserRequestFactory;
import bio.terra.controller.JournalApi;
import bio.terra.model.IamResourceTypeEnum;
import bio.terra.model.JournalEntryModel;
import bio.terra.service.auth.iam.IamAction;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.journal.JournalService;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.annotations.Api;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;

@Controller
@Api(tags = {"journal"})
public class JournalApiController implements JournalApi {
  private final IamService iamService;
  private final JournalService journalService;
  private final AuthenticatedUserRequestFactory authenticatedUserRequestFactory;
  private final HttpServletRequest request;
  private final ObjectMapper objectMapper;

  @Autowired
  public JournalApiController(
      AuthenticatedUserRequestFactory authenticatedUserRequestFactory,
      HttpServletRequest request,
      IamService iamService,
      JournalService journalService,
      ObjectMapper objectMapper) {
    this.authenticatedUserRequestFactory = authenticatedUserRequestFactory;
    this.iamService = iamService;
    this.journalService = journalService;
    this.objectMapper = objectMapper;
    this.request = request;
  }

  @Override
  public ResponseEntity<List<JournalEntryModel>> retrieveJournalEntries(
      UUID resourceKey, IamResourceTypeEnum resourceType, Integer offset, Integer limit) {
    IamResourceType iamResourceType =
        IamResourceTypeCODEC.toIamResourceType(resourceType.toString());
    iamService.verifyAuthorization(
        getAuthenticatedInfo(), iamResourceType, resourceKey.toString(), IamAction.VIEW_JOURNAL);
    List<JournalEntryModel> journalEntries =
        journalService.getJournalEntries(resourceKey, iamResourceType, offset, limit);
    return new ResponseEntity<>(journalEntries, HttpStatus.OK);
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
