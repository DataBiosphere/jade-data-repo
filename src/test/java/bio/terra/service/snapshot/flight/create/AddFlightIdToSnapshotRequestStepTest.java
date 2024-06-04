package bio.terra.service.snapshot.flight.create;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.service.snapshotbuilder.SnapshotRequestDao;
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
class AddFlightIdToSnapshotRequestStepTest {

  @Mock private SnapshotRequestDao snapshotRequestDao;
  private static final UUID SNAPSHOT_REQUEST_ID = UUID.randomUUID();
  private AddFlightIdToSnapshotRequestStep addFlightIdToSnapshotRequestStep;
  private final FlightContext flightContext = mock(FlightContext.class);

  @BeforeEach
  public void beforeEach() {
    addFlightIdToSnapshotRequestStep =
        new AddFlightIdToSnapshotRequestStep(snapshotRequestDao, SNAPSHOT_REQUEST_ID);
  }

  @Test
  void doStep() throws InterruptedException {
    String flightId = "flightId";
    when(flightContext.getFlightId()).thenReturn(flightId);
    StepResult result = addFlightIdToSnapshotRequestStep.doStep(flightContext);
    verify(snapshotRequestDao).updateFlightId(SNAPSHOT_REQUEST_ID, flightId);
    assertEquals(StepResult.getStepResultSuccess(), result);
  }

  @Test
  void undoStep() throws InterruptedException {
    assertEquals(
        StepResult.getStepResultSuccess(),
        addFlightIdToSnapshotRequestStep.undoStep(flightContext));
  }
}
