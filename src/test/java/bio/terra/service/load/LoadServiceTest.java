package bio.terra.service.load;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.model.BulkLoadHistoryModel;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.load.LoadService.LoadHistoryIterator;
import bio.terra.service.load.exception.LoadLockFailureException;
import bio.terra.service.load.exception.LoadLockedException;
import bio.terra.service.load.flight.LoadMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class LoadServiceTest {
  @Mock private LoadDao loadDao;
  @Mock private FlightContext flightContext;
  private LoadService loadService;
  private static final UUID LOAD_ID = UUID.randomUUID();
  private static final String LOAD_TAG = "a-load-tag";
  private static final UUID DATASET_ID = UUID.randomUUID();
  private static final LoadLockKey LOAD_LOCK_KEY = new LoadLockKey(LOAD_TAG, DATASET_ID);
  private static final String LOCKING_FLIGHT_ID = "a-locking-flight-id";

  @BeforeEach
  void setup() {
    loadService = new LoadService(loadDao);
  }

  @Test
  void lockLoad() {
    LoadLock expectedLoadLock = new LoadLock(LOAD_ID, LOAD_LOCK_KEY, LOCKING_FLIGHT_ID);
    when(loadDao.lockLoad(LOAD_LOCK_KEY, LOCKING_FLIGHT_ID)).thenReturn(expectedLoadLock);
    assertThat(loadService.lockLoad(LOAD_LOCK_KEY, LOCKING_FLIGHT_ID), equalTo(LOAD_ID));
  }

  @Test
  void lockLoad_LoadLockedException() {
    doThrow(LoadLockedException.class).when(loadDao).lockLoad(LOAD_LOCK_KEY, LOCKING_FLIGHT_ID);
    assertThrows(
        LoadLockedException.class, () -> loadService.lockLoad(LOAD_LOCK_KEY, LOCKING_FLIGHT_ID));
  }

  @Test
  void unlockLoad() {
    loadService.unlockLoad(LOAD_LOCK_KEY, LOCKING_FLIGHT_ID);
    verify(loadDao).unlockLoad(LOAD_LOCK_KEY, LOCKING_FLIGHT_ID);
  }

  @Test
  void computeLoadTagTest() {
    String loadTag = loadService.computeLoadTag(null);
    assertThat("generated load tag", loadTag, startsWith("load-at-"));
    loadTag = loadService.computeLoadTag(LOAD_TAG);
    assertThat("pass through load tag", loadTag, equalTo(LOAD_TAG));
  }

  private void mockInputParams(boolean hasLoadTag) {
    FlightMap inputParams = new FlightMap();
    inputParams.put(JobMapKeys.DATASET_ID.getKeyName(), DATASET_ID);
    if (hasLoadTag) {
      inputParams.put(LoadMapKeys.LOAD_TAG, LOAD_TAG);
    }
    when(flightContext.getInputParameters()).thenReturn(inputParams);
  }

  @Test
  void getLoadLockKey_workingMap() {
    mockInputParams(false);

    FlightMap workingMap = new FlightMap();
    workingMap.put(LoadMapKeys.LOAD_TAG, LOAD_TAG);
    when(flightContext.getWorkingMap()).thenReturn(workingMap);

    LoadLockKey actualLoadLockKey = loadService.getLoadLockKey(flightContext);
    assertThat(
        "Load tag retrieved from working map",
        actualLoadLockKey,
        equalTo(new LoadLockKey(LOAD_TAG, DATASET_ID)));
  }

  @Test
  void getLoadLockKey_inputParameters() {
    mockInputParams(true);

    LoadLockKey actualLoadLockKey = loadService.getLoadLockKey(flightContext);
    assertThat(
        "Load tag retrieved from input params",
        actualLoadLockKey,
        equalTo(new LoadLockKey(LOAD_TAG, DATASET_ID)));
  }

  @Test
  void getLoadLockKey_LoadLockFailureException() {
    mockInputParams(false);
    when(flightContext.getWorkingMap()).thenReturn(new FlightMap());
    assertThrows(
        LoadLockFailureException.class,
        () -> loadService.getLoadLockKey(flightContext),
        "Fails when load tag can't be found in input or working maps");
  }

  @Test
  void loadHistoryIterator_fromDao() {
    int chunkSize = 2;
    List<BulkLoadHistoryModel> loadHistoryInputs =
        List.of(
            new BulkLoadHistoryModel().sourcePath("source1").targetPath("target1"),
            new BulkLoadHistoryModel().sourcePath("source2").targetPath("target2"),
            new BulkLoadHistoryModel().sourcePath("source3").targetPath("target3"));
    when(loadDao.bulkLoadFileArraySize(LOAD_ID)).thenReturn(loadHistoryInputs.size());

    LoadHistoryIterator loadHistoryIterator = loadService.loadHistoryIterator(LOAD_ID, chunkSize);
    List<BulkLoadHistoryModel> loadHistoryModels = new ArrayList<>();

    when(loadDao.makeLoadHistoryArray(LOAD_ID, chunkSize, 0))
        .thenReturn(loadHistoryInputs.subList(0, 2));
    when(loadDao.makeLoadHistoryArray(LOAD_ID, chunkSize, 1))
        .thenReturn(loadHistoryInputs.subList(2, 3));

    while (loadHistoryIterator.hasNext()) {
      loadHistoryModels.addAll(loadHistoryIterator.next());
    }

    assertThat(
        "load history models were returned",
        loadHistoryModels.stream().map(BulkLoadHistoryModel::getTargetPath).toList(),
        containsInAnyOrder("target1", "target2", "target3"));
  }

  @Test
  void loadHistoryIterator_fromFlightContext() {
    List<BulkLoadHistoryModel> loadHistoryInputs =
        List.of(
            new BulkLoadHistoryModel().sourcePath("source1").targetPath("target1"),
            new BulkLoadHistoryModel().sourcePath("source2").targetPath("target2"),
            new BulkLoadHistoryModel().sourcePath("source3").targetPath("target3"));
    LoadHistoryIterator loadHistoryIterator = loadService.loadHistoryIterator(loadHistoryInputs, 2);
    List<BulkLoadHistoryModel> loadHistoryModels = new ArrayList<>();

    while (loadHistoryIterator.hasNext()) {
      loadHistoryModels.addAll(loadHistoryIterator.next());
    }

    assertThat(
        "load history models were returned",
        loadHistoryModels.stream().map(BulkLoadHistoryModel::getTargetPath).toList(),
        containsInAnyOrder("target1", "target2", "target3"));
  }
}
