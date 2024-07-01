package bio.terra.service.snapshot.flight.create;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.SnapshotAccessRequestResponse;
import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.service.snapshotbuilder.SnapshotRequestDao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@Tag(Unit.TAG)
@ExtendWith(MockitoExtension.class)
class CreateSnapshotAddEmailsToSamGroupStepTest {

  @Mock private IamService iamService;
  @Mock private SnapshotRequestDao snapshotRequestDao;
  @Mock private FlightContext flightContext;
  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();
  private static final String RESEARCHER_EMAIL = "researcher@gmail.com";

  private static final String GROUP_NAME = "groupName";

  private CreateSnapshotAddEmailsToSamGroupStep step;
  private UUID snapshotRequestId;

  @BeforeEach
  void setUp() {
    FlightMap workingMap = new FlightMap();
    snapshotRequestId = UUID.randomUUID();
    step =
        new CreateSnapshotAddEmailsToSamGroupStep(
            TEST_USER, iamService, snapshotRequestDao, snapshotRequestId);
    workingMap.put(SnapshotWorkingMapKeys.SNAPSHOT_FIRECLOUD_GROUP_NAME, GROUP_NAME);
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
  }

  @Test
  void doStep() throws InterruptedException {
    var emailsToAdd = List.of(TEST_USER.getEmail(), RESEARCHER_EMAIL);
    var request = new SnapshotAccessRequestResponse().createdBy(RESEARCHER_EMAIL);
    when(snapshotRequestDao.getById(snapshotRequestId)).thenReturn(request);
    assertEquals(step.doStep(flightContext), StepResult.getStepResultSuccess());
    verify(iamService)
        .overwriteGroupPolicyEmails(GROUP_NAME, IamRole.MEMBER.toString(), emailsToAdd);
  }
}
