package bio.terra.pact.provider;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import au.com.dius.pact.provider.junit5.PactVerificationContext;
import au.com.dius.pact.provider.junitsupport.Provider;
import au.com.dius.pact.provider.junitsupport.State;
import au.com.dius.pact.provider.junitsupport.loader.PactBroker;
import au.com.dius.pact.provider.spring.junit5.MockMvcTestTarget;
import au.com.dius.pact.provider.spring.junit5.PactVerificationSpringProvider;
import bio.terra.app.controller.SnapshotsApiController;
import bio.terra.common.category.Pact;
import bio.terra.common.iam.AuthenticatedUserRequestFactory;
import bio.terra.model.SnapshotModel;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.auth.iam.exception.IamForbiddenException;
import bio.terra.service.dataset.AssetModelValidator;
import bio.terra.service.dataset.IngestRequestValidator;
import bio.terra.service.filedata.FileService;
import bio.terra.service.job.JobService;
import bio.terra.service.snapshot.SnapshotRequestValidator;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.exception.SnapshotNotFoundException;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;

@Tag(Pact.TAG)
@Provider("tdr")
@PactBroker
@ContextConfiguration(classes = SnapshotsApiController.class)
@WebMvcTest
public class DataRepoProviderTest {

  @Autowired private MockMvc mvc;

  @MockBean private JobService jobService;
  @MockBean private SnapshotRequestValidator snapshotRequestValidator;
  @MockBean private SnapshotService snapshotService;
  @MockBean private IamService iamService;
  @MockBean private IngestRequestValidator ingestRequestValidator;
  @MockBean private FileService fileService;
  @MockBean private AuthenticatedUserRequestFactory authenticatedUserRequestFactory;
  @MockBean private AssetModelValidator assetModelValidator;

  private static final UUID SNAPSHOT_ID = UUID.fromString("12345678-abc9-012d-3456-e7fab89cd01e");

  @BeforeEach
  void before(PactVerificationContext context) {
    context.setTarget(new MockMvcTestTarget(mvc));
  }

  @TestTemplate
  @ExtendWith(PactVerificationSpringProvider.class)
  void pactVerificationTestTemplate(PactVerificationContext context) {
    context.verifyInteraction();
  }

  @State("snapshot doesn't exist")
  void getNonexistentSnapshot() {
    when(snapshotService.retrieveSnapshotModel(eq(SNAPSHOT_ID), any(), any()))
        .thenThrow(
            new SnapshotNotFoundException(
                String.format("Snapshot not found - id: %s", SNAPSHOT_ID)));
  }

  @State("user does not have access to snapshot")
  void noAccessToSnapshot() {
    doThrow(new IamForbiddenException("User does not have required action"))
        .when(snapshotService)
        .verifySnapshotReadable(eq(SNAPSHOT_ID), any());
  }

  @State("user has access to snapshot")
  void successfulSnapshot() {
    when(snapshotService.retrieveSnapshotModel(eq(SNAPSHOT_ID), any(), any()))
        .thenReturn(
            new SnapshotModel()
                .id(SNAPSHOT_ID)
                .name("snapshot name")
                .description("SNAPSHOT_DESCRIPTION"));
  }
}
