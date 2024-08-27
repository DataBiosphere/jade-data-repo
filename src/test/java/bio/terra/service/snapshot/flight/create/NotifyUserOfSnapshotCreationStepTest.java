package bio.terra.service.snapshot.flight.create;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.snapshotbuilder.SnapshotBuilderService;
import bio.terra.service.snapshotbuilder.SnapshotBuilderTestData;
import bio.terra.service.snapshotbuilder.SnapshotRequestDao;
import bio.terra.stairway.StepStatus;
import java.util.UUID;
import org.broadinstitute.dsde.workbench.client.sam.model.UserIdInfo;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@Tag(Unit.TAG)
@ExtendWith(MockitoExtension.class)
class NotifyUserOfSnapshotCreationStepTest {
  @Mock private SnapshotBuilderService snapshotBuilderService;
  @Mock private SnapshotRequestDao snapshotRequestDao;
  @Mock private IamService iamService;
  private static final String TOKEN = "some-access-token";
  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.userRequest(TOKEN);

  @Test
  void doStep() throws InterruptedException {
    var id = UUID.randomUUID();
    var step =
        new NotifyUserOfSnapshotCreationStep(
            TEST_USER, snapshotBuilderService, snapshotRequestDao, iamService, id);
    var request = SnapshotBuilderTestData.createAccessRequest();
    var user = new UserIdInfo().userSubjectId("subjectId");
    when(snapshotRequestDao.getById(id)).thenReturn(request);
    when(iamService.getUserIds(request.createdBy())).thenReturn(user);
    assertThat(step.doStep(null).getStepStatus(), is(StepStatus.STEP_RESULT_SUCCESS));
    verify(snapshotBuilderService).notifySnapshotReady(TEST_USER, user.getUserSubjectId(), id);
  }
}
