package bio.terra.service.snapshot.flight.create;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.model.SnapshotRequestContentsModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.common.CommonMapKeys;
import bio.terra.service.dataset.AssetSpecification;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class CreateSnapshotPrimaryDataQueryInterfaceTest {
  @Mock FlightContext flightContext;
  @Mock DatasetService datasetService;
  @Mock CreateSnapshotPrimaryDataQueryInterface createSnapshotPrimaryDataQueryInterface;

  @Test
  void prepareQueryAndCreateSnapshotByRequestId() throws InterruptedException {
    FlightMap workingMap = new FlightMap();
    AssetSpecification asset = new AssetSpecification().name("person_visit");
    workingMap.put(SnapshotWorkingMapKeys.ASSET, asset);
    String sqlQuery = "sqlQuery";
    workingMap.put(SnapshotWorkingMapKeys.SQL_QUERY, sqlQuery);
    Snapshot snapshot = new Snapshot();
    FlightMap inputParameters = new FlightMap();
    inputParameters.put(CommonMapKeys.CREATED_AT, 1000);

    when(flightContext.getWorkingMap()).thenReturn(workingMap);
    when(flightContext.getInputParameters()).thenReturn(inputParameters);
    SnapshotRequestModel snapshotRequestModel =
        new SnapshotRequestModel()
            .addContentsItem(
                new SnapshotRequestContentsModel()
                    .mode(SnapshotRequestContentsModel.ModeEnum.BYREQUESTID));

    // mock out call to createSnapshotPrimaryData
    when(createSnapshotPrimaryDataQueryInterface.createSnapshotPrimaryData(
            eq(flightContext), eq(asset), eq(snapshot), eq(sqlQuery), any()))
        .thenReturn(StepResult.getStepResultSuccess());
    when(createSnapshotPrimaryDataQueryInterface.prepareQueryAndCreateSnapshot(
            any(), any(), any(), any()))
        .thenCallRealMethod();

    assertThat(
        createSnapshotPrimaryDataQueryInterface.prepareQueryAndCreateSnapshot(
            flightContext, snapshot, snapshotRequestModel, datasetService),
        is(StepResult.getStepResultSuccess()));
  }
}
