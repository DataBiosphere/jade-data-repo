package bio.terra.pact.provider;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import au.com.dius.pact.provider.junit5.PactVerificationContext;
import au.com.dius.pact.provider.junitsupport.Provider;
import au.com.dius.pact.provider.junitsupport.State;
import au.com.dius.pact.provider.junitsupport.loader.PactBroker;
import au.com.dius.pact.provider.spring.junit5.PactVerificationSpringProvider;
import bio.terra.common.category.Pact;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.common.iam.AuthenticatedUserRequestFactory;
import bio.terra.model.SnapshotModel;
import bio.terra.service.auth.iam.exception.IamForbiddenException;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.exception.SnapshotNotFoundException;
import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@Tag(Pact.TAG)
@Provider("tdr")
@PactBroker
@ExtendWith(SpringExtension.class)
@AutoConfigureMockMvc
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
public class DataRepoProviderTest {

  @MockBean SnapshotService snapshotService;

  @MockBean AuthenticatedUserRequestFactory authenticatedUserRequestFactory;
  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();

  @TestTemplate
  @ExtendWith(PactVerificationSpringProvider.class)
  void pactVerificationTestTemplate(PactVerificationContext context) {
    context.verifyInteraction();
  }

  @State("snapshot doesn't exist")
  void getNonexistentSnapshot() {
    when(snapshotService.retrieveSnapshotModel(
            eq(UUID.fromString("12345678-abc9-012d-3456-e7fab89cd01e")), any(), any()))
        .thenThrow(
            new SnapshotNotFoundException(
                "Snapshot not found - id: 12345678-abc9-012d-3456-e7fab89cd01e"));
  }

  @State("user does not have access to snapshot")
  void noAccessToSnapshot() {
    doThrow(new IamForbiddenException("User does not have required action"))
        .when(snapshotService)
        .verifySnapshotReadable(any(), any());
  }

  @State("user has access to snapshot")
  void successfulSnapshot() {
    UUID id = UUID.fromString("12345678-abc9-012d-3456-e7fab89cd01e");
    when(snapshotService.retrieveSnapshotModel(any(), any(), any()))
        .thenReturn(
            new SnapshotModel().id(id).name("snapshot name").description("SNAPSHOT_DESCRIPTION"));
  }
}
