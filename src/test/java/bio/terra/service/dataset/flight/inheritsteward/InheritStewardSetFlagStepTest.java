package bio.terra.service.dataset.flight.inheritsteward;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class InheritStewardSetFlagStepTest {

  @Mock private DatasetDao datasetDao;
  private UUID datasetId;
  private boolean enableInheritSteward;
  private InheritStewardSetFlagStep step;
  private FlightContext context;

  @BeforeEach
  void setUp() {
    datasetId = UUID.randomUUID();
    enableInheritSteward = true;
    step = new InheritStewardSetFlagStep(datasetDao, datasetId, enableInheritSteward);
    context = mock(FlightContext.class);
  }

  @Test
  void testDoStepSuccess() {
    when(datasetDao.setInheritSteward(datasetId, enableInheritSteward)).thenReturn(true);
    StepResult result = step.doStep(context);
    assertEquals(StepResult.getStepResultSuccess(), result);
  }

  @Test
  void testDoStepFailure() {
    when(datasetDao.setInheritSteward(datasetId, enableInheritSteward)).thenReturn(false);
    StepResult result = step.doStep(context);
    assertEquals(StepStatus.STEP_RESULT_FAILURE_FATAL, result.getStepStatus());
  }

  @Test
  void testUndoStepSuccess() {
    when(datasetDao.setInheritSteward(datasetId, !enableInheritSteward)).thenReturn(true);
    StepResult result = step.undoStep(context);
    assertEquals(StepResult.getStepResultSuccess(), result);
  }

  @Test
  void testUndoStepFailure() {
    when(datasetDao.setInheritSteward(datasetId, !enableInheritSteward)).thenReturn(false);
    StepResult result = step.undoStep(context);
    assertEquals(StepStatus.STEP_RESULT_FAILURE_FATAL, result.getStepStatus());
  }
}
