package bio.terra.service.dataset.flight.ingest;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import bio.terra.common.PdaoLoadStatistics;
import bio.terra.common.category.Unit;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.IngestRequestModel.FormatEnum;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class IngestUtilsTest {
  private static final UUID DATASET_ID = UUID.randomUUID();
  private static final Dataset DATASET = new Dataset().id(DATASET_ID);
  @Mock private FlightContext context;
  @Mock private DatasetService datasetService;
  private FlightMap inputParameters;

  @BeforeEach
  void beforeEach() {
    inputParameters = new FlightMap();
  }

  private void initializeDatasetIdParameter() {
    inputParameters.put(JobMapKeys.DATASET_ID.getKeyName(), DATASET_ID);
    when(context.getInputParameters()).thenReturn(inputParameters);
  }

  @Test
  void getDatasetId() {
    initializeDatasetIdParameter();
    assertThat(IngestUtils.getDatasetId(context), equalTo(DATASET_ID));
  }

  @Test
  void getDatasetId_IllegalStateException() {
    when(context.getInputParameters()).thenReturn(inputParameters);
    assertThrows(IllegalStateException.class, () -> IngestUtils.getDatasetId(context));
  }

  @Test
  void getDataset() {
    initializeDatasetIdParameter();
    when(datasetService.retrieveForIngest(DATASET_ID)).thenReturn(DATASET);
    assertThat(IngestUtils.getDataset(context, datasetService), equalTo(DATASET));
  }

  @Test
  void getDataset_IllegalStateException() {
    when(context.getInputParameters()).thenReturn(inputParameters);
    assertThrows(
        IllegalStateException.class, () -> IngestUtils.getDataset(context, datasetService));
  }

  @ParameterizedTest
  @EnumSource(names = {"CSV", "ARRAY", "JSON"})
  void testJsonTypeIngest(FormatEnum format) {
    inputParameters.put(JobMapKeys.REQUEST.getKeyName(), new IngestRequestModel().format(format));
    if (format == FormatEnum.CSV) {
      assertFalse(
          IngestUtils.isJsonTypeIngest(inputParameters),
          format + " ingest is not considered json-type");
    } else {
      assertTrue(
          IngestUtils.isJsonTypeIngest(inputParameters),
          format + " ingest is considered json-type");
    }
  }

  @Test
  void testShouldIgnoreUserSpecifiedRowIds() {
    // We should not find ourselves here: ingests default to append mode if unspecified.
    FlightMap flightMapNoUpdateStrategy = createFlightMap(null);
    assertFalse(
        IngestUtils.shouldIgnoreUserSpecifiedRowIds(flightMapNoUpdateStrategy),
        "Ingests with unspecified update strategy can specify their own row IDs");

    FlightMap flightMapAppend = createFlightMap(IngestRequestModel.UpdateStrategyEnum.APPEND);
    assertFalse(
        IngestUtils.shouldIgnoreUserSpecifiedRowIds(flightMapAppend),
        "Ingests in append mode can specify their own row IDs");

    FlightMap flightMapReplace = createFlightMap(IngestRequestModel.UpdateStrategyEnum.REPLACE);
    assertTrue(
        IngestUtils.shouldIgnoreUserSpecifiedRowIds(flightMapReplace),
        "Ingests in replace mode will have any specified row IDs unset");

    FlightMap flightMapMerge = createFlightMap(IngestRequestModel.UpdateStrategyEnum.MERGE);
    assertTrue(
        IngestUtils.shouldIgnoreUserSpecifiedRowIds(flightMapMerge),
        "Ingests in merge mode will have any specified row IDs unset");
  }

  /**
   * @param updateStrategy to specify on a new stub ingest request
   * @return a new FlightMap whose ingest request contains the provided update strategy
   */
  private FlightMap createFlightMap(IngestRequestModel.UpdateStrategyEnum updateStrategy) {
    IngestRequestModel ingestRequest = new IngestRequestModel().updateStrategy(updateStrategy);
    inputParameters.put(JobMapKeys.REQUEST.getKeyName(), ingestRequest);
    return inputParameters;
  }

  @Test
  void putIngestStatistics() {
    when(context.getWorkingMap()).thenReturn(new FlightMap());
    PdaoLoadStatistics ingestStatistics =
        new PdaoLoadStatistics(123, 456, Instant.EPOCH, Instant.EPOCH);
    IngestUtils.putIngestStatistics(context, ingestStatistics);
    PdaoLoadStatistics actual = IngestUtils.getIngestStatistics(context);
    assertEquals(ingestStatistics.getRowCount(), actual.getRowCount());
    assertEquals(ingestStatistics.getBadRecords(), actual.getBadRecords());
    assertEquals(ingestStatistics.getStartTime(), actual.getStartTime());
    assertEquals(ingestStatistics.getEndTime(), actual.getEndTime());
  }
}
