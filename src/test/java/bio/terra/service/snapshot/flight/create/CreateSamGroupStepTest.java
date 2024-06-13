package bio.terra.service.snapshot.flight.create;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.verify;

import bio.terra.common.category.Unit;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.auth.iam.IamService;
import bio.terra.stairway.StepResult;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class CreateSamGroupStepTest {

  @Mock private IamService iamService;

  @Test
  void doStep() throws InterruptedException {
    SnapshotRequestModel snapshotReq = new SnapshotRequestModel().name("test");
    CreateSamGroupStep step = new CreateSamGroupStep(iamService, snapshotReq);
    assertEquals(StepResult.getStepResultSuccess(), step.doStep(null));
    verify(iamService).createGroup(snapshotReq.getName());
  }

  @Test
  void undoStep() throws InterruptedException {
    SnapshotRequestModel snapshotReq = new SnapshotRequestModel().name("test");
    CreateSamGroupStep step = new CreateSamGroupStep(iamService, snapshotReq);
    assertEquals(StepResult.getStepResultSuccess(), step.undoStep(null));
    verify(iamService).deleteGroup(snapshotReq.getName());
  }
}
