package bio.terra.service.load;

import bio.terra.model.BulkLoadFileModel;
import bio.terra.model.BulkLoadFileResultModel;
import bio.terra.model.BulkLoadFileState;
import bio.terra.model.BulkLoadHistoryModel;
import bio.terra.model.BulkLoadResultModel;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.filedata.FSFileInfo;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.load.exception.LoadLockFailureException;
import bio.terra.service.load.flight.LoadMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class LoadService {
  private final LoadDao loadDao;

  @Autowired
  public LoadService(LoadDao loadDao) {
    this.loadDao = loadDao;
  }

  public UUID lockLoad(LoadLockKey loadLockKey, String flightId) {
    LoadLock loadLock = loadDao.lockLoad(loadLockKey, flightId);
    return loadLock.id();
  }

  public void unlockLoad(LoadLockKey loadLockKey, String flightId) {
    loadDao.unlockLoad(loadLockKey, flightId);
  }

  public void unlockLoad(UUID datasetId, String flightId) {
    loadDao.unlockLoad(new LoadLockKey(null, datasetId), flightId);
  }

  public void populateFiles(UUID loadId, List<BulkLoadFileModel> loadFileModelList) {
    loadDao.populateFiles(loadId, loadFileModelList);
  }

  public void populateFiles(
      UUID loadId, Stream<BulkLoadFileModel> loadFileModelStream, int batchSize) {
    loadDao.populateFiles(loadId, loadFileModelStream, batchSize);
  }

  public void cleanFiles(UUID loadId) {
    loadDao.cleanFiles(loadId);
  }

  public List<LoadFile> findRunningLoads(UUID loadId) {
    return loadDao.findLoadsByState(loadId, BulkLoadFileState.RUNNING, null);
  }

  /**
   * @param inputTag may be null or blank
   * @return either valid inputTag or generated date-time tag.
   */
  public String computeLoadTag(String inputTag) {
    if (StringUtils.isEmpty(inputTag)) {
      return "load-at-"
          + Instant.now().atZone(ZoneId.of("Z")).format(DateTimeFormatter.ISO_INSTANT);
    }
    return inputTag;
  }

  /**
   * @param context Flight context with the following:
   *     <ul>
   *       <li>{@link LoadMapKeys#LOAD_TAG} in the input parameters or working map
   *       <li>{@link JobMapKeys#DATASET_ID} in the input parameters
   *     </ul>
   *
   * @return a {@link LoadLockKey} constructed from the flight's context, which this flight will
   *     attempt to lock while loading files.
   * @throws LoadLockFailureException if no load tag can be found in the flight context
   */
  public LoadLockKey getLoadLockKey(FlightContext context) throws LoadLockFailureException {
    return new LoadLockKey(getLoadTag(context), IngestUtils.getDatasetId(context));
  }

  private String getLoadTag(FlightContext context) throws LoadLockFailureException {
    FlightMap inputParameters = context.getInputParameters();
    String key = LoadMapKeys.LOAD_TAG;
    String loadTag = inputParameters.get(key, String.class);
    if (StringUtils.isEmpty(loadTag)) {
      FlightMap workingMap = context.getWorkingMap();
      loadTag = workingMap.get(key, String.class);
      if (StringUtils.isEmpty(loadTag)) {
        throw new LoadLockFailureException(
            "Expected %s in working map or inputs, but did not find it".formatted(key));
      }
    }
    return loadTag;
  }

  // -- wrap the DAO interface in the service interface --
  public LoadCandidates findCandidates(UUID loadId, int candidatesToFind) {
    return loadDao.findCandidates(loadId, candidatesToFind);
  }

  public List<LoadFile> getFailedLoads(UUID loadId, int maxRecords) {
    return loadDao.getFailedLoads(loadId, maxRecords);
  }

  public void setLoadFileSucceeded(
      UUID loadId, String targetPath, String fileId, FSFileInfo fileInfo) {
    loadDao.setLoadFileSucceeded(loadId, targetPath, fileId, fileInfo);
  }

  public void setLoadFileFailed(UUID loadId, String targetPath, String error) {
    loadDao.setLoadFileFailed(loadId, targetPath, error);
  }

  public void setLoadFileRunning(UUID loadId, String targetPath, String flightId) {
    loadDao.setLoadFileRunning(loadId, targetPath, flightId);
  }

  public void setLoadFileNotTried(UUID loadId, String targetPath) {
    loadDao.setLoadFileNotTried(loadId, targetPath);
  }

  public BulkLoadResultModel makeBulkLoadResult(UUID loadId) {
    return loadDao.makeBulkLoadResult(loadId);
  }

  public List<BulkLoadFileResultModel> makeBulkLoadFileArray(UUID loadId) {
    return loadDao.makeBulkLoadFileArray(loadId);
  }

  public LoadHistoryIterator loadHistoryIterator(UUID loadId, int chunkSize) {
    return new LoadHistoryIterator(loadId, chunkSize);
  }

  public LoadHistoryIterator loadHistoryIterator(
      List<BulkLoadHistoryModel> backingList, int chunkSize) {
    return new LoadHistoryIterator(backingList, chunkSize);
  }

  /**
   * A convenience class wrapping the getting of load history table rows in an Iterator. The
   * Iterator's elements are a list of BulkLoadHistoryModel chunk of the full results retrieved from
   * the database.
   */
  public class LoadHistoryIterator implements Iterator<List<BulkLoadHistoryModel>> {

    private final UUID loadId;
    private final int chunkSize;
    private final int loadHistorySize;
    private final List<List<BulkLoadHistoryModel>> backingList;

    private int currentChunk;

    public LoadHistoryIterator(UUID loadId, int chunkSize) {
      this.loadId = loadId;
      this.chunkSize = chunkSize;
      this.loadHistorySize = loadDao.bulkLoadFileArraySize(loadId);
      this.currentChunk = 0;
      this.backingList = null;
    }

    public LoadHistoryIterator(List<BulkLoadHistoryModel> backingList, int chunkSize) {
      this.loadId = null;
      this.chunkSize = chunkSize;
      this.loadHistorySize = backingList.size();
      this.currentChunk = 0;
      this.backingList = ListUtils.partition(backingList, chunkSize);
    }

    @Override
    public boolean hasNext() {
      return currentChunk * chunkSize < loadHistorySize;
    }

    @Override
    public List<BulkLoadHistoryModel> next() {
      if (backingList == null) {
        return loadDao.makeLoadHistoryArray(loadId, chunkSize, currentChunk++);
      }
      if (currentChunk < backingList.size()) {
        return backingList.get(currentChunk++);
      }
      return null;
    }
  }
}
