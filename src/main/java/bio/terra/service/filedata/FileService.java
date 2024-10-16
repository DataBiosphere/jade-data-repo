package bio.terra.service.filedata;

import static bio.terra.service.common.azure.StorageTableName.DATASET;
import static bio.terra.service.common.azure.StorageTableName.SNAPSHOT;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.CollectionType;
import bio.terra.common.exception.FeatureNotImplementedException;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BulkLoadArrayRequestModel;
import bio.terra.model.BulkLoadRequestModel;
import bio.terra.model.DRSChecksum;
import bio.terra.model.DirectoryDetailModel;
import bio.terra.model.FileDetailModel;
import bio.terra.model.FileLoadModel;
import bio.terra.model.FileModel;
import bio.terra.model.FileModelType;
import bio.terra.service.auth.iam.IamAction;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.azure.tables.TableDao;
import bio.terra.service.filedata.exception.BulkLoadFileMaxExceededException;
import bio.terra.service.filedata.exception.FileSystemCorruptException;
import bio.terra.service.filedata.exception.FileSystemExecutionException;
import bio.terra.service.filedata.flight.delete.FileDeleteFlight;
import bio.terra.service.filedata.flight.ingest.FileIngestBulkFlight;
import bio.terra.service.filedata.flight.ingest.FileIngestFlight;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.job.JobService;
import bio.terra.service.load.LoadService;
import bio.terra.service.load.flight.LoadMapKeys;
import bio.terra.service.profile.ProfileService;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAuthInfo;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotProject;
import bio.terra.service.snapshot.SnapshotService;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class FileService {

  private final JobService jobService;
  private final FireStoreDao fileDao;
  private final DatasetService datasetService;
  private final SnapshotService snapshotService;
  private final LoadService loadService;
  private final ConfigurationService configService;
  private final TableDao tableDao;
  private final ResourceService resourceService;
  private final ProfileService profileService;

  @Autowired
  public FileService(
      JobService jobService,
      FireStoreDao fileDao,
      DatasetService datasetService,
      SnapshotService snapshotService,
      LoadService loadService,
      ConfigurationService configService,
      TableDao tableDao,
      ResourceService resourceService,
      ProfileService profileService) {
    this.fileDao = fileDao;
    this.datasetService = datasetService;
    this.jobService = jobService;
    this.snapshotService = snapshotService;
    this.loadService = loadService;
    this.configService = configService;
    this.tableDao = tableDao;
    this.resourceService = resourceService;
    this.profileService = profileService;
  }

  public String deleteFile(String datasetId, String fileId, AuthenticatedUserRequest userReq) {
    String description = "Delete file from dataset " + datasetId + " file " + fileId;
    return jobService
        .newJob(description, FileDeleteFlight.class, null, userReq)
        .addParameter(JobMapKeys.DATASET_ID.getKeyName(), datasetId)
        .addParameter(JobMapKeys.FILE_ID.getKeyName(), fileId)
        .addParameter(JobMapKeys.IAM_RESOURCE_TYPE.getKeyName(), IamResourceType.DATASET)
        .addParameter(JobMapKeys.IAM_RESOURCE_ID.getKeyName(), datasetId)
        .addParameter(JobMapKeys.IAM_ACTION.getKeyName(), IamAction.INGEST_DATA)
        .submit();
  }

  public String ingestFile(
      String datasetId, FileLoadModel fileLoad, AuthenticatedUserRequest userReq) {
    String loadTag = loadService.computeLoadTag(fileLoad.getLoadTag());
    fileLoad.setLoadTag(loadTag);
    String description = "Ingest file " + fileLoad.getTargetPath();
    return jobService
        .newJob(description, FileIngestFlight.class, fileLoad, userReq)
        .addParameter(JobMapKeys.DATASET_ID.getKeyName(), datasetId)
        .addParameter(LoadMapKeys.LOAD_TAG, loadTag)
        .addParameter(JobMapKeys.IAM_RESOURCE_TYPE.getKeyName(), IamResourceType.DATASET)
        .addParameter(JobMapKeys.IAM_RESOURCE_ID.getKeyName(), datasetId)
        .addParameter(JobMapKeys.IAM_ACTION.getKeyName(), IamAction.INGEST_DATA)
        .submit();
  }

  public String ingestBulkFile(
      String datasetId, BulkLoadRequestModel loadModel, AuthenticatedUserRequest userReq) {
    String loadTag = loadService.computeLoadTag(loadModel.getLoadTag());
    loadModel.setLoadTag(loadTag);
    String description =
        "Bulk ingest from control file: "
            + loadModel.getLoadControlFile()
            + "  LoadTag: "
            + loadTag;

    return jobService
        .newJob(description, FileIngestBulkFlight.class, loadModel, userReq)
        .addParameter(LoadMapKeys.IS_ARRAY, false)
        .addParameter(JobMapKeys.DATASET_ID.getKeyName(), datasetId)
        .addParameter(LoadMapKeys.LOAD_TAG, loadTag)
        .addParameter(
            LoadMapKeys.DRIVER_WAIT_SECONDS,
            configService.getParameterValue(ConfigEnum.LOAD_DRIVER_WAIT_SECONDS))
        .addParameter(
            LoadMapKeys.LOAD_HISTORY_COPY_CHUNK_SIZE,
            configService.getParameterValue(ConfigEnum.LOAD_HISTORY_COPY_CHUNK_SIZE))
        .addParameter(
            LoadMapKeys.LOAD_HISTORY_WAIT_SECONDS,
            configService.getParameterValue(ConfigEnum.LOAD_HISTORY_WAIT_SECONDS))
        .addParameter(JobMapKeys.IAM_RESOURCE_TYPE.getKeyName(), IamResourceType.DATASET)
        .addParameter(JobMapKeys.IAM_RESOURCE_ID.getKeyName(), datasetId)
        .addParameter(JobMapKeys.IAM_ACTION.getKeyName(), IamAction.INGEST_DATA)
        .submit();
  }

  public String ingestBulkFileArray(
      String datasetId, BulkLoadArrayRequestModel loadArray, AuthenticatedUserRequest userReq) {
    String loadTag = loadService.computeLoadTag(loadArray.getLoadTag());
    loadArray.setLoadTag(loadTag);
    String description =
        "Bulk ingest from array of "
            + loadArray.getLoadArray().size()
            + " files. LoadTag: "
            + loadTag;

    int filesMax = configService.getParameterValue(ConfigEnum.LOAD_BULK_ARRAY_FILES_MAX);
    int inArraySize = loadArray.getLoadArray().size();
    if (inArraySize > filesMax) {
      throw new BulkLoadFileMaxExceededException(
          "Maximum number of files in a bulk load array is "
              + filesMax
              + "; request array contains "
              + inArraySize);
    }
    return jobService
        .newJob(description, FileIngestBulkFlight.class, loadArray, userReq)
        .addParameter(LoadMapKeys.IS_ARRAY, true)
        .addParameter(JobMapKeys.DATASET_ID.getKeyName(), datasetId)
        .addParameter(LoadMapKeys.LOAD_TAG, loadTag)
        .addParameter(
            LoadMapKeys.DRIVER_WAIT_SECONDS,
            configService.getParameterValue(ConfigEnum.LOAD_DRIVER_WAIT_SECONDS))
        .addParameter(
            LoadMapKeys.LOAD_HISTORY_COPY_CHUNK_SIZE,
            configService.getParameterValue(ConfigEnum.LOAD_HISTORY_COPY_CHUNK_SIZE))
        .addParameter(
            LoadMapKeys.LOAD_HISTORY_WAIT_SECONDS,
            configService.getParameterValue(ConfigEnum.LOAD_HISTORY_WAIT_SECONDS))
        .addParameter(JobMapKeys.IAM_RESOURCE_TYPE.getKeyName(), IamResourceType.DATASET)
        .addParameter(JobMapKeys.IAM_RESOURCE_ID.getKeyName(), datasetId)
        .addParameter(JobMapKeys.IAM_ACTION.getKeyName(), IamAction.INGEST_DATA)
        .submit();
  }

  public List<FileModel> listDatasetFiles(String datasetId, int offset, int limit) {
    Dataset dataset = datasetService.retrieve(UUID.fromString(datasetId));
    CloudPlatformWrapper cloudPlatformWrapper = CloudPlatformWrapper.of(dataset.getCloudPlatform());
    if (cloudPlatformWrapper.isGcp()) {
      try {
        return fileDao.batchRetrieveFiles(dataset, dataset, offset, limit);
      } catch (InterruptedException ex) {
        throw new FileSystemExecutionException(ex);
      }
    } else {
      String collectionId = DATASET.toTableName(dataset.getId());
      AzureStorageAuthInfo storageAuthInfo = resourceService.getDatasetStorageAuthInfo(dataset);
      return tableDao.batchRetrieveFiles(
          collectionId, storageAuthInfo, datasetId, storageAuthInfo, offset, limit);
    }
  }

  public List<FileModel> listSnapshotFiles(String snapshotId, int offset, int limit) {
    Snapshot snapshot = snapshotService.retrieve(UUID.fromString(snapshotId));
    Dataset dataset = snapshot.getSourceDataset();
    CloudPlatformWrapper cloudPlatformWrapper =
        CloudPlatformWrapper.of(snapshot.getCloudPlatform());
    if (cloudPlatformWrapper.isGcp()) {
      try {
        return fileDao.batchRetrieveFiles(snapshot, dataset, offset, limit);
      } catch (InterruptedException ex) {
        throw new FileSystemExecutionException(ex);
      }
    } else {
      String collectionId = SNAPSHOT.toTableName(snapshot.getId());
      AzureStorageAuthInfo storageAuthInfo = resourceService.getSnapshotStorageAuthInfo(snapshot);
      AzureStorageAuthInfo datasetStorageAuthInfo =
          resourceService.getDatasetStorageAuthInfo(dataset);
      return tableDao.batchRetrieveFiles(
          collectionId,
          storageAuthInfo,
          dataset.getId().toString(),
          datasetStorageAuthInfo,
          offset,
          limit);
    }
  }

  // -- dataset lookups --
  // depth == -1 means expand the entire sub-tree from this node
  // depth == 0 means no expansion - just this node
  // depth >= 1 means expand N levels
  public FileModel lookupFile(String datasetId, String fileId, int depth) {
    try {
      return fileModelFromFSItem(lookupFSItem(datasetId, fileId, depth));
    } catch (InterruptedException ex) {
      throw new FileSystemExecutionException(ex);
    }
  }

  public FileModel lookupPath(String datasetId, String path, int depth) {
    FSItem fsItem = null;
    try {
      fsItem = lookupFSItemByPath(datasetId, path, depth);
    } catch (InterruptedException ex) {
      throw new FileSystemExecutionException(ex);
    }
    return fileModelFromFSItem(fsItem);
  }

  public Optional<FileModel> lookupOptionalPath(String datasetId, String path, int depth) {
    Dataset dataset = datasetService.retrieve(UUID.fromString(datasetId));
    CloudPlatformWrapper cloudPlatformWrapper =
        CloudPlatformWrapper.of(dataset.getDatasetSummary().getStorageCloudPlatform());
    final Optional<FSItem> file;
    if (cloudPlatformWrapper.isGcp()) {
      try {
        file = fileDao.lookupOptionalPath(dataset, path, depth);
      } catch (InterruptedException ex) {
        throw new FileSystemExecutionException(ex);
      }
    } else {
      AzureStorageAuthInfo storageAuthInfo = resourceService.getDatasetStorageAuthInfo(dataset);
      file = tableDao.lookupOptionalPath(dataset.getId(), path, storageAuthInfo, depth);
    }
    return file.map(this::fileModelFromFSItem);
  }

  FSItem lookupFSItem(String datasetId, String fileId, int depth) throws InterruptedException {
    Dataset dataset = datasetService.retrieve(UUID.fromString(datasetId));
    CloudPlatformWrapper cloudPlatformWrapper =
        CloudPlatformWrapper.of(dataset.getDatasetSummary().getStorageCloudPlatform());
    if (cloudPlatformWrapper.isGcp()) {
      return fileDao.retrieveById(dataset, fileId, depth);
    } else if (cloudPlatformWrapper.isAzure()) {
      AzureStorageAuthInfo storageAuthInfo = resourceService.getDatasetStorageAuthInfo(dataset);

      return tableDao.retrieveById(
          CollectionType.DATASET,
          UUID.fromString(datasetId),
          UUID.fromString(datasetId),
          fileId,
          depth,
          storageAuthInfo,
          storageAuthInfo);
    } else {
      throw new FeatureNotImplementedException("Cloud platform not implemented");
    }
  }

  FSItem lookupFSItemByPath(String datasetId, String path, int depth) throws InterruptedException {
    Dataset dataset = datasetService.retrieve(UUID.fromString(datasetId));
    CloudPlatformWrapper cloudPlatformWrapper =
        CloudPlatformWrapper.of(dataset.getDatasetSummary().getStorageCloudPlatform());
    if (cloudPlatformWrapper.isGcp()) {
      return fileDao.retrieveByPath(dataset, path, depth);
    } else {
      AzureStorageAuthInfo storageAuthInfo = resourceService.getDatasetStorageAuthInfo(dataset);
      return tableDao.retrieveByPath(UUID.fromString(datasetId), path, depth, storageAuthInfo);
    }
  }

  // -- snapshot lookups --
  // TODO - Q4 - Handle snapshot in Azure
  public FileModel lookupSnapshotFile(String snapshotId, String fileId, int depth) {
    try {
      // note: this method only returns snapshots that are NOT exclusively locked
      SnapshotProject snapshot =
          snapshotService.retrieveSnapshotProject(UUID.fromString(snapshotId));
      return fileModelFromFSItem(lookupSnapshotFSItem(snapshot, fileId, depth));
    } catch (InterruptedException ex) {
      throw new FileSystemExecutionException(ex);
    }
  }

  public FileModel lookupSnapshotPath(String snapshotId, String path, int depth) {
    FSItem fsItem = null;
    try {
      fsItem = lookupSnapshotFSItemByPath(snapshotId, path, depth);
    } catch (InterruptedException ex) {
      throw new FileSystemExecutionException(ex);
    }
    return fileModelFromFSItem(fsItem);
  }

  FSItem lookupSnapshotFSItem(SnapshotProject snapshot, String fileId, int depth)
      throws InterruptedException {
    CloudPlatformWrapper cloudPlatformWrapper =
        CloudPlatformWrapper.of(snapshot.getCloudPlatform());
    if (cloudPlatformWrapper.isGcp()) {
      return fileDao.retrieveBySnapshotAndId(snapshot, fileId, depth);
    } else {
      // TODO: this will get expensive if we query a lot.  We'll need to optimize this
      AzureStorageAuthInfo storageAuthInfo =
          resourceService.getSnapshotStorageAuthInfo(snapshot.getProfileId(), snapshot.getId());

      // TODO Cache these values.  Very expensive lookups
      Dataset dataset =
          datasetService.retrieve(snapshot.getSourceDatasetProjects().iterator().next().getId());
      AzureStorageAuthInfo datasetTableStorageAuthInfo =
          resourceService.getDatasetStorageAuthInfo(dataset);

      return tableDao.retrieveById(
          CollectionType.SNAPSHOT,
          dataset.getId(),
          snapshot.getId(),
          fileId,
          depth,
          storageAuthInfo,
          datasetTableStorageAuthInfo);
    }
  }

  FSItem lookupSnapshotFSItemByPath(String snapshotId, String path, int depth)
      throws InterruptedException {
    Snapshot snapshot = snapshotService.retrieve(UUID.fromString(snapshotId));
    return fileDao.retrieveByPath(snapshot, path, depth);
  }

  public FileModel fileModelFromFSItem(FSItem fsItem) {
    FileModel fileModel =
        new FileModel()
            .fileId(fsItem.getFileId().toString())
            .collectionId(fsItem.getCollectionId().toString())
            .path(fsItem.getPath())
            .size(fsItem.getSize())
            .created(fsItem.getCreatedDate().toString())
            .description(fsItem.getDescription())
            .checksums(makeChecksums(fsItem));

    if (fsItem instanceof FSFile) {
      fileModel.fileType(FileModelType.FILE);

      FSFile fsFile = (FSFile) fsItem;
      fileModel.fileDetail(
          new FileDetailModel()
              .datasetId(fsFile.getDatasetId().toString())
              .accessUrl(fsFile.getCloudPath())
              .mimeType(fsFile.getMimeType())
              .loadTag(fsFile.getLoadTag()));
    } else if (fsItem instanceof FSDir) {
      fileModel.fileType(FileModelType.DIRECTORY);
      FSDir fsDir = (FSDir) fsItem;
      DirectoryDetailModel directoryDetail =
          new DirectoryDetailModel().enumerated(fsDir.isEnumerated());
      if (fsDir.isEnumerated()) {
        directoryDetail.contents(new ArrayList<>());
        for (FSItem fsContentsItem : fsDir.getContents()) {
          FileModel itemModel = fileModelFromFSItem(fsContentsItem);
          directoryDetail.addContentsItem(itemModel);
        }
      }
      fileModel.directoryDetail(directoryDetail);
    } else {
      throw new FileSystemCorruptException("Entry type is totally wrong; we shouldn't be here");
    }

    return fileModel;
  }

  // We use the DRSChecksum model to represent the checksums in the repository
  // API's FileModel to return the set of checksums for a file.
  /*
   * WARNING: if making any changes to this method make sure to notify the #dsp-batch channel! Describe the change and
   * any consequences downstream to DRS clients.
   */
  static List<DRSChecksum> makeChecksums(ChecksumInterface checksum) {
    String fsItemCrc32c = checksum.getChecksumCrc32c();
    List<DRSChecksum> checksums = new ArrayList<>();
    if (fsItemCrc32c != null) {
      DRSChecksum checksumCrc32 = new DRSChecksum().checksum(fsItemCrc32c).type("crc32c");
      checksums.add(checksumCrc32);
    }
    String fsItemMd5 = checksum.getChecksumMd5();
    if (fsItemMd5 != null) {
      DRSChecksum checksumMd5 = new DRSChecksum().checksum(fsItemMd5).type("md5");
      checksums.add(checksumMd5);
    }
    return checksums;
  }
}
