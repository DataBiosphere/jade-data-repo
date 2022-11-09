package bio.terra.service.snapshot.flight.duos;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import bio.terra.common.category.Unit;
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
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class RemoveDuosFirecloudReaderStepTest {

  @MockBean private IamService iamService;
  @Mock private FlightContext flightContext;

  private static final UUID SNAPSHOT_ID = UUID.randomUUID();

  private AuthenticatedUserRequest userReq;
  private DuosFirecloudGroupModel duosFirecloudGroupPrev;
  private RemoveDuosFirecloudReaderStep step;

  @Before
  public void setup() {
    userReq =
        AuthenticatedUserRequest.builder()
            .setSubjectId("RemoveDuosFirecloudReaderStepTest")
            .setEmail("RemoveDuosFirecloudReaderStepTest@unit.com")
            .setToken("token")
            .build();

    duosFirecloudGroupPrev = DuosFixtures.duosFirecloudGroupFromDb("DUOS-123456");

    step =
        new RemoveDuosFirecloudReaderStep(iamService, userReq, SNAPSHOT_ID, duosFirecloudGroupPrev);
  }

  @Test
  public void testDoAndUndoStep() throws InterruptedException {
    StepResult doResult = step.doStep(flightContext);
    assertEquals(doResult.getStepStatus(), StepStatus.STEP_RESULT_SUCCESS);
    verify(iamService, times(1))
        .deletePolicyMember(
            userReq,
            IamResourceType.DATASNAPSHOT,
            SNAPSHOT_ID,
            IamRole.READER.toString(),
            duosFirecloudGroupPrev.getFirecloudGroupEmail());
    verifyNoInteractions(flightContext);

    StepResult undoResult = step.undoStep(flightContext);
    assertEquals(undoResult.getStepStatus(), StepStatus.STEP_RESULT_SUCCESS);
    verify(iamService, times(1))
        .addPolicyMember(
            userReq,
            IamResourceType.DATASNAPSHOT,
            SNAPSHOT_ID,
            IamRole.READER.toString(),
            duosFirecloudGroupPrev.getFirecloudGroupEmail());
    verifyNoInteractions(flightContext);
  }
}
