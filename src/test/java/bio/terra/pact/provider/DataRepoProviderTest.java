package bio.terra.pact.provider;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import au.com.dius.pact.provider.junit5.PactVerificationContext;
import au.com.dius.pact.provider.junitsupport.Provider;
import au.com.dius.pact.provider.junitsupport.State;
import au.com.dius.pact.provider.junitsupport.loader.PactBroker;
import au.com.dius.pact.provider.spring.junit5.PactVerificationSpringProvider;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.common.iam.AuthenticatedUserRequestFactory;
import bio.terra.service.auth.iam.exception.IamForbiddenException;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.exception.SnapshotNotFoundException;
import java.util.UUID;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@Provider("tdr-provider")
@PactBroker
@ExtendWith(SpringExtension.class)
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

  @State("snapshot does not exist")
  void getNonexistentSnapshot() {
    when(snapshotService.retrieveSnapshotModel(
            UUID.fromString("12345678-abc9-012d-3456-e7fab89cd01e"), TEST_USER))
        .thenThrow(new SnapshotNotFoundException("Snapshot not found"));
  }

  @State("user does not have access to snapshot")
  void noAccessToSnapshot() {
    doThrow(new IamForbiddenException("User does not have required action"))
        .when(snapshotService)
        .verifySnapshotReadable(UUID.fromString("12345678-abc9-012d-3456-e7fab89cd01e"), TEST_USER);
  }
}
