package bio.terra.service.snapshot.flight.duos;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.fixtures.DuosFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.DuosFirecloudGroupModel;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.auth.iam.IamService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.context.ActiveProfiles;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class RemoveDuosFirecloudReaderStepTest {

  @Mock private IamService iamService;
  @Mock private FlightContext flightContext;

  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();
  private static final UUID SNAPSHOT_ID = UUID.randomUUID();
  private static final DuosFirecloudGroupModel DUOS_FIRECLOUD_GROUP_PREV =
      DuosFixtures.mockDuosFirecloudGroupFromDb("DUOS-123456");

  private RemoveDuosFirecloudReaderStep step;

  @Before
  public void setup() {
    step =
        new RemoveDuosFirecloudReaderStep(
            iamService, TEST_USER, SNAPSHOT_ID, DUOS_FIRECLOUD_GROUP_PREV);
  }

  @Test
  public void testDoAndUndoStep() throws InterruptedException {
    StepResult doResult = step.doStep(flightContext);
    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(iamService)
        .deletePolicyMember(
            TEST_USER,
            IamResourceType.DATASNAPSHOT,
            SNAPSHOT_ID,
            IamRole.READER.toString(),
            DUOS_FIRECLOUD_GROUP_PREV.getFirecloudGroupEmail());
    verifyNoInteractions(flightContext);

    StepResult undoResult = step.undoStep(flightContext);
    assertThat(undoResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(iamService)
        .addPolicyMember(
            TEST_USER,
            IamResourceType.DATASNAPSHOT,
            SNAPSHOT_ID,
            IamRole.READER.toString(),
            DUOS_FIRECLOUD_GROUP_PREV.getFirecloudGroupEmail());
    verifyNoInteractions(flightContext);
  }
}
