package bio.terra.service.snapshot.flight.duos;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.DuosFixtures;
import bio.terra.model.DuosFirecloudGroupModel;
import bio.terra.service.job.OptionalStep;
import bio.terra.stairway.FlightMap;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.ApplicationContext;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class SnapshotUpdateDuosDatasetFlightTest {

  @Mock private ApplicationContext context;

  private static final String DUOS_ID = "DUOS-123456";
  private static final DuosFirecloudGroupModel FIRECLOUD_GROUP_PREV =
      DuosFixtures.createDbFirecloudGroup(DUOS_ID);

  @Test
  void testConstructFlightLinkDuosDataset() {
    FlightMap inputParameters = new FlightMap();
    inputParameters.put(SnapshotDuosMapKeys.DUOS_ID, DUOS_ID);
    inputParameters.put(SnapshotDuosMapKeys.FIRECLOUD_GROUP_PREV, null);

    var flight = new SnapshotUpdateDuosDatasetFlight(inputParameters, context);

    assertThat(
        "Flight only has steps for linking a new DUOS dataset",
        getFlightStepNames(flight),
        contains(
            "RetrieveDuosFirecloudGroupStep",
            "IfNoGroupRetrievedStep",
            "IfNoGroupRetrievedStep",
            "IfNoGroupRetrievedStep",
            "AddDuosFirecloudReaderStep",
            "UpdateSnapshotDuosFirecloudGroupIdStep"));
    assertThat(
        "Firecloud group creation, record, and sync steps are optional",
        getFlightOptionalStepNames(flight),
        contains(
            "CreateDuosFirecloudGroupStep",
            "RecordDuosFirecloudGroupStep",
            "SyncDuosFirecloudGroupStep"));
  }

  @Test
  void testConstructFlightUpdateExistingDuosDatasetLink() {
    FlightMap inputParameters = new FlightMap();
    inputParameters.put(SnapshotDuosMapKeys.DUOS_ID, DUOS_ID);
    inputParameters.put(SnapshotDuosMapKeys.FIRECLOUD_GROUP_PREV, FIRECLOUD_GROUP_PREV);

    var flight = new SnapshotUpdateDuosDatasetFlight(inputParameters, context);

    assertThat(
        "Flight has steps for unlinking the current DUOS dataset and linking a new one",
        getFlightStepNames(flight),
        contains(
            "RemoveDuosFirecloudReaderStep",
            "RetrieveDuosFirecloudGroupStep",
            "IfNoGroupRetrievedStep",
            "IfNoGroupRetrievedStep",
            "IfNoGroupRetrievedStep",
            "AddDuosFirecloudReaderStep",
            "UpdateSnapshotDuosFirecloudGroupIdStep"));
    assertThat(
        "Firecloud group creation, record, and sync steps are optional",
        getFlightOptionalStepNames(flight),
        contains(
            "CreateDuosFirecloudGroupStep",
            "RecordDuosFirecloudGroupStep",
            "SyncDuosFirecloudGroupStep"));
  }

  @Test
  void testConstructFlightUnlinkDuosDataset() {
    FlightMap inputParameters = new FlightMap();
    inputParameters.put(SnapshotDuosMapKeys.DUOS_ID, null);
    inputParameters.put(SnapshotDuosMapKeys.FIRECLOUD_GROUP_PREV, FIRECLOUD_GROUP_PREV);

    var flight = new SnapshotUpdateDuosDatasetFlight(inputParameters, context);

    assertThat(
        "Flight only has steps for unlinking the current DUOS dataset",
        getFlightStepNames(flight),
        contains("RemoveDuosFirecloudReaderStep", "UpdateSnapshotDuosFirecloudGroupIdStep"));
    assertThat(
        "We do not need to create a Firecloud group when only unlinking a DUOS dataset",
        getFlightOptionalStepNames(flight),
        empty());
  }

  private List<String> getFlightStepNames(SnapshotUpdateDuosDatasetFlight flight) {
    return flight.getSteps().stream()
        .map(step -> step.getClass().getSimpleName())
        .collect(Collectors.toList());
  }

  private List<String> getFlightOptionalStepNames(SnapshotUpdateDuosDatasetFlight flight) {
    return flight.getSteps().stream()
        .filter(step -> step instanceof OptionalStep)
        .map(step -> ((OptionalStep) step).getStep().getClass().getSimpleName())
        .collect(Collectors.toList());
  }
}
