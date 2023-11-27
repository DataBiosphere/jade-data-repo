package bio.terra.pact.provider;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import au.com.dius.pact.provider.junit5.PactVerificationContext;
import au.com.dius.pact.provider.junitsupport.Provider;
import au.com.dius.pact.provider.junitsupport.State;
import au.com.dius.pact.provider.junitsupport.loader.PactBroker;
import au.com.dius.pact.provider.junitsupport.loader.PactBrokerConsumerVersionSelectors;
import au.com.dius.pact.provider.junitsupport.loader.SelectorBuilder;
import au.com.dius.pact.provider.spring.junit5.MockMvcTestTarget;
import au.com.dius.pact.provider.spring.junit5.PactVerificationSpringProvider;
import bio.terra.app.controller.GlobalExceptionHandler;
import bio.terra.app.controller.SnapshotsApiController;
import bio.terra.common.category.Pact;
import bio.terra.common.exception.ForbiddenException;
import bio.terra.common.iam.AuthenticatedUserRequestFactory;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotRetrieveIncludeModel;
import bio.terra.model.TableModel;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.dataset.AssetModelValidator;
import bio.terra.service.dataset.IngestRequestValidator;
import bio.terra.service.filedata.FileService;
import bio.terra.service.job.JobService;
import bio.terra.service.snapshot.SnapshotRequestValidator;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.exception.SnapshotNotFoundException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;

/**
 * Establishing SnapshotApiController Provider states via mocking for use in verifying contracts
 * made against these APIs by external Consumers.
 */
@Tag(Pact.TAG)
@ActiveProfiles(Pact.PROFILE)
@Provider(Pact.PACTICIPANT)
@PactBroker
@ContextConfiguration(classes = {SnapshotsApiController.class, GlobalExceptionHandler.class})
@WebMvcTest
class SnapshotsApiControllerTest {

  @Autowired private MockMvc mvc;

  @MockBean private JobService jobService;
  @MockBean private SnapshotRequestValidator snapshotRequestValidator;
  @MockBean private SnapshotService snapshotService;
  @MockBean private IamService iamService;
  @MockBean private IngestRequestValidator ingestRequestValidator;
  @MockBean private FileService fileService;
  @MockBean private AuthenticatedUserRequestFactory authenticatedUserRequestFactory;
  @MockBean private AssetModelValidator assetModelValidator;

  @PactBrokerConsumerVersionSelectors
  public static SelectorBuilder consumerVersionSelectors() {
    // Select consumer pacts published from default branch or pacts marked as deployed or released.
    // If you wish to pick up Pacts from a consumer's feature branch for development purposes or PR
    // runs, and your consumer is publishing such Pacts under their feature branch name, you can add
    // the following to the SelectorBuilder:
    //   .branch("consumer-feature-branch-name")
    return new SelectorBuilder().mainBranch().deployedOrReleased();
  }

  @BeforeEach
  void before(PactVerificationContext context) {
    context.setTarget(new MockMvcTestTarget(mvc));
  }

  @TestTemplate
  @ExtendWith(PactVerificationSpringProvider.class)
  void pactVerificationTestTemplate(PactVerificationContext context) {
    context.verifyInteraction();
  }

  @State("snapshot with given id doesn't exist")
  void mockRetrieveSnapshotModel_snapshotNotFound(Map<?, ?> parameters) {
    when(snapshotService.retrieveSnapshotModel(eq(idFromParameters(parameters)), any(), any()))
        .thenThrow(SnapshotNotFoundException.class);
  }

  @State("user does not have access to snapshot with given id")
  void mockVerifySnapshotReadable_forbidden(Map<?, ?> parameters) {
    doThrow(ForbiddenException.class)
        .when(snapshotService)
        .verifySnapshotReadable(eq(idFromParameters(parameters)), any());
  }

  @State("user has access to snapshot with given id")
  void mockRetrieveSnapshotModel_success(Map<?, ?> parameters) {
    UUID snapshotId = idFromParameters(parameters);
    when(snapshotService.retrieveSnapshotModel(
            eq(snapshotId), eq(List.of(SnapshotRetrieveIncludeModel.TABLES)), any()))
        .thenReturn(
            new SnapshotModel()
                .id(snapshotId)
                .name("A snapshot name")
                .addTablesItem(new TableModel().name("A table name")));
  }

  private UUID idFromParameters(Map<?, ?> parameters) {
    return UUID.fromString(parameters.get("id").toString());
  }
}