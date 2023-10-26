package bio.terra.service.snapshot.flight.authDomain;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.snapshot.exception.AuthDomainGroupNotFoundException;
import bio.terra.service.snapshot.exception.SnapshotAuthDomainExistsException;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.List;
import java.util.UUID;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.context.ActiveProfiles;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class AddSnapshotAuthDomainStepTest {

  @Mock private IamService iamService;
  @Mock private FlightContext flightContext;
  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();
  private static final UUID SNAPSHOT_ID = UUID.randomUUID();
  private static final List<String> userGroups = List.of("group1", "group2");

  @Test
  public void testDoAndUndoStepSucceeds() throws InterruptedException {
    AddSnapshotAuthDomainStep step =
        new AddSnapshotAuthDomainStep(iamService, TEST_USER, SNAPSHOT_ID, userGroups);
    StepResult doResult = step.doStep(flightContext);
    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(iamService)
        .patchAuthDomain(TEST_USER, IamResourceType.DATASNAPSHOT, SNAPSHOT_ID, userGroups);
  }

  @Test
  public void testSnapshotAuthDomainExistsError() throws InterruptedException {
    when(iamService.retrieveAuthDomain(TEST_USER, IamResourceType.DATASNAPSHOT, SNAPSHOT_ID))
        .thenReturn(userGroups);

    AddSnapshotAuthDomainStep step =
        new AddSnapshotAuthDomainStep(iamService, TEST_USER, SNAPSHOT_ID, userGroups);
    StepResult doResult = step.doStep(flightContext);
    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_FAILURE_FATAL));
    assertThat(doResult.getException().get(), instanceOf(SnapshotAuthDomainExistsException.class));
  }

  @Test
  public void testUserGroupNotFoundError() throws InterruptedException {
    AuthDomainGroupNotFoundException ex =
        new AuthDomainGroupNotFoundException("auth domain not found");
    when(iamService.retrieveAuthDomain(TEST_USER, IamResourceType.DATASNAPSHOT, SNAPSHOT_ID))
        .thenReturn(List.of());
    doThrow(ex)
        .when(iamService)
        .patchAuthDomain(TEST_USER, IamResourceType.DATASNAPSHOT, SNAPSHOT_ID, userGroups);

    AddSnapshotAuthDomainStep step =
        new AddSnapshotAuthDomainStep(iamService, TEST_USER, SNAPSHOT_ID, userGroups);
    assertThrows(AuthDomainGroupNotFoundException.class, () -> step.doStep(flightContext));
  }
}