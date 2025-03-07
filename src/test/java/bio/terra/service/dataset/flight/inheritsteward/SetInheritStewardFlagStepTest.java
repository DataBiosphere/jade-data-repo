package bio.terra.service.dataset.flight.inheritsteward;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class SetInheritStewardFlagStepTest {

  @Mock private DatasetDao datasetDao;
  private UUID datasetId;
  private SetInheritStewardFlagStep step;
  @Mock private FlightContext context;

  @BeforeEach
  void setUp() {
    datasetId = UUID.randomUUID();
    step = new SetInheritStewardFlagStep(datasetDao, datasetId, true);
  }

  @Test
  void doStep() {
    when(datasetDao.setInheritSteward(datasetId, true)).thenReturn(true);
    StepResult result = step.doStep(context);
    assertEquals(StepResult.getStepResultSuccess(), result);
  }

  @Test
  void undoStep() {
    when(datasetDao.setInheritSteward(datasetId, false)).thenReturn(true);
    StepResult result = step.undoStep(context);
    assertEquals(StepResult.getStepResultSuccess(), result);
  }
}
