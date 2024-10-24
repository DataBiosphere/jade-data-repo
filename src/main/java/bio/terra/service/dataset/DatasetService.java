package bio.terra.service.dataset;

import static bio.terra.common.PdaoConstant.PDAO_ROW_ID_COLUMN;

import bio.terra.app.controller.DatasetsApiController;
import bio.terra.app.usermetrics.BardEventProperties;
import bio.terra.app.usermetrics.UserLoggingMetrics;
import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.CollectionType;
import bio.terra.common.Column;
import bio.terra.common.SqlSortDirection;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.AccessInfoModel;
import bio.terra.model.AssetModel;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BulkLoadHistoryModel;
import bio.terra.model.CloudPlatform;
import bio.terra.model.ColumnStatisticsModel;
import bio.terra.model.DataDeletionRequest;
import bio.terra.model.DatasetDataModel;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetPatchRequestModel;
import bio.terra.model.DatasetRequestAccessIncludeModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetSchemaUpdateModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.EnumerateDatasetModel;
import bio.terra.model.EnumerateSortByParam;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.IngestRequestModel.FormatEnum;
import bio.terra.model.ResourceLocks;
import bio.terra.model.TagCount;
import bio.terra.model.TagCountResultModel;
import bio.terra.model.TagUpdateRequestModel;
import bio.terra.model.TransactionCloseModel.ModeEnum;
import bio.terra.model.TransactionCreateModel;
import bio.terra.model.TransactionModel;
import bio.terra.model.UnlockResourceRequest;
import bio.terra.service.auth.iam.IamAction;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.dataset.exception.DatasetDataException;
import bio.terra.service.dataset.exception.DatasetNotFoundException;
import bio.terra.service.dataset.exception.IngestFailureException;
import bio.terra.service.dataset.flight.create.AddAssetSpecFlight;
import bio.terra.service.dataset.flight.create.DatasetCreateFlight;
import bio.terra.service.dataset.flight.datadelete.DatasetDataDeleteFlight;
import bio.terra.service.dataset.flight.delete.DatasetDeleteFlight;
import bio.terra.service.dataset.flight.delete.RemoveAssetSpecFlight;
import bio.terra.service.dataset.flight.ingest.DatasetIngestFlight;
import bio.terra.service.dataset.flight.ingest.IngestMapKeys;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.dataset.flight.ingest.scratch.DatasetScratchFilePrepareFlight;
import bio.terra.service.dataset.flight.lock.DatasetLockFlight;
import bio.terra.service.dataset.flight.transactions.TransactionCommitFlight;
import bio.terra.service.dataset.flight.transactions.TransactionOpenFlight;
import bio.terra.service.dataset.flight.transactions.TransactionRollbackFlight;
import bio.terra.service.dataset.flight.unlock.DatasetUnlockFlight;
import bio.terra.service.dataset.flight.update.DatasetSchemaUpdateFlight;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.filedata.azure.SynapseDataResultModel;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.filedata.azure.util.BlobSasTokenOptions;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.job.JobService;
import bio.terra.service.load.LoadService;
import bio.terra.service.load.flight.LoadMapKeys;
import bio.terra.service.profile.ProfileDao;
import bio.terra.service.profile.ProfileService;
import bio.terra.service.profile.exception.ProfileNotFoundException;
import bio.terra.service.resourcemanagement.MetadataDataAccessUtils;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.service.snapshot.exception.AssetNotFoundException;
import bio.terra.service.tabulardata.azure.StorageTableService;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDataResultModel;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDatasetPdao;
import bio.terra.service.tabulardata.google.bigquery.BigQueryPdao;
import bio.terra.service.tabulardata.google.bigquery.BigQueryTransactionPdao;
import bio.terra.stairway.ShortUUID;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class DatasetService {
  private static final Logger logger = LoggerFactory.getLogger(DatasetService.class);
  private final DatasetJsonConversion datasetJsonConversion;
  private final DatasetDao datasetDao;
  private final JobService jobService; // for handling flight response
  private final LoadService loadService;
  private final ProfileDao profileDao;
  private final StorageTableService storageTableService;
  private final BigQueryTransactionPdao bigQueryTransactionPdao;
  private final BigQueryDatasetPdao bigQueryDatasetPdao;
  private final ResourceService resourceService;
  private final GcsPdao gcsPdao;
  private final ObjectMapper objectMapper;
  private final AzureBlobStorePdao azureBlobStorePdao;
  private final ProfileService profileService;
  private final UserLoggingMetrics loggingMetrics;
  private final IamService iamService;
  private final DatasetTableDao datasetTableDao;
  private final AzureSynapsePdao azureSynapsePdao;
  private final MetadataDataAccessUtils metadataDataAccessUtils;

  @Autowired
  public DatasetService(
      DatasetJsonConversion datasetJsonConversion,
      DatasetDao datasetDao,
      JobService jobService,
      LoadService loadService,
      ProfileDao profileDao,
      StorageTableService storageTableService,
      BigQueryTransactionPdao bigQueryTransactionPdao,
      BigQueryDatasetPdao bigQueryDatasetPdao,
      ResourceService resourceService,
      GcsPdao gcsPdao,
      ObjectMapper objectMapper,
      AzureBlobStorePdao azureBlobStorePdao,
      ProfileService profileService,
      UserLoggingMetrics loggingMetrics,
      IamService iamService,
      DatasetTableDao datasetTableDao,
      AzureSynapsePdao azureSynapsePdao,
      MetadataDataAccessUtils metadataDataAccessUtils) {
    this.datasetJsonConversion = datasetJsonConversion;
    this.datasetDao = datasetDao;
    this.jobService = jobService;
    this.loadService = loadService;
    this.profileDao = profileDao;
    this.storageTableService = storageTableService;
    this.bigQueryTransactionPdao = bigQueryTransactionPdao;
    this.bigQueryDatasetPdao = bigQueryDatasetPdao;
    this.resourceService = resourceService;
    this.gcsPdao = gcsPdao;
    this.objectMapper = objectMapper;
    this.azureBlobStorePdao = azureBlobStorePdao;
    this.profileService = profileService;
    this.loggingMetrics = loggingMetrics;
    this.iamService = iamService;
    this.datasetTableDao = datasetTableDao;
    this.azureSynapsePdao = azureSynapsePdao;
    this.metadataDataAccessUtils = metadataDataAccessUtils;
  }

  public String createDataset(
      DatasetRequestModel datasetRequest, AuthenticatedUserRequest userReq) {
    String description = "Create dataset " + datasetRequest.getName();
    UUID defaultProfileId = datasetRequest.getDefaultProfileId();
    loggingMetrics.set(BardEventProperties.BILLING_PROFILE_ID_FIELD_NAME, defaultProfileId);
    return jobService
        .newJob(description, DatasetCreateFlight.class, datasetRequest, userReq)
        .addParameter(JobMapKeys.IAM_RESOURCE_TYPE.getKeyName(), IamResourceType.SPEND_PROFILE)
        .addParameter(JobMapKeys.IAM_RESOURCE_ID.getKeyName(), defaultProfileId)
        .addParameter(JobMapKeys.IAM_ACTION.getKeyName(), IamAction.LINK)
        .submit();
  }

  /**
   * Fetch existing Dataset object.
   *
   * @param id in UUID format
   * @return a Dataset object
   */
  public Dataset retrieve(UUID id) {
    return datasetDao.retrieve(id);
  }

  /**
   * Fetch existing Dataset object populated only as necessary to facilitate data ingestion (i.e.
   * without relationships or assets which are constructed via costly database calls).
   *
   * @param id in UUID format
   * @return a Dataset object
   */
  public Dataset retrieveForIngest(UUID id) {
    return datasetDao.retrieve(id, false, false);
  }

  /**
   * Fetch existing Dataset object using the name.
   *
   * @param name
   * @return a Dataset object
   */
  public Dataset retrieveByName(String name) {
    return datasetDao.retrieveByName(name);
  }

  /**
   * Convenience wrapper around fetching an existing Dataset object and converting it to a Model
   * object. Unlike the Dataset object, the Model object includes a reference to the associated
   * cloud project.
   *
   * @param id in UUID format
   * @return a DatasetModel = API output-friendly representation of the Dataset
   */
  public DatasetModel retrieveDatasetModel(
      UUID id,
      AuthenticatedUserRequest userRequest,
      List<DatasetRequestAccessIncludeModel> include) {
    Dataset dataset = retrieve(id);
    return retrieveModel(dataset, userRequest, include);
  }

  public DatasetModel retrieveDatasetModel(UUID id, AuthenticatedUserRequest userRequest) {
    return retrieveDatasetModel(id, userRequest, getDefaultIncludes());
  }

  /**
   * Convenience wrapper to grab the dataset model from the dataset object, avoids having to
   * retrieve the dataset a second time if you already have it
   *
   * @param dataset the dataset being passed in
   * @param userRequest the user making the request
   * @return a DatasetModel = API output-friendly representation of the Dataset
   */
  public DatasetModel retrieveModel(Dataset dataset, AuthenticatedUserRequest userRequest) {
    return retrieveModel(dataset, userRequest, getDefaultIncludes());
  }

  public DatasetModel retrieveModel(
      Dataset dataset,
      AuthenticatedUserRequest userRequest,
      List<DatasetRequestAccessIncludeModel> include) {
    return datasetJsonConversion.populateDatasetModelFromDataset(dataset, include, userRequest);
  }

  /**
   * @return summary model of the dataset
   */
  public DatasetSummaryModel retrieveDatasetSummary(UUID id) {
    DatasetSummary datasetSummary = datasetDao.retrieveSummaryById(id);
    return datasetSummary.toModel();
  }

  public EnumerateDatasetModel enumerate(
      int offset,
      int limit,
      EnumerateSortByParam sort,
      SqlSortDirection direction,
      String filter,
      String region,
      Map<UUID, Set<IamRole>> idsAndRoles,
      List<String> tags) {
    if (idsAndRoles.isEmpty()) {
      return new EnumerateDatasetModel().total(0).items(List.of());
    }
    var datasetEnum =
        datasetDao.enumerate(
            offset, limit, sort, direction, filter, region, idsAndRoles.keySet(), tags);

    List<DatasetSummaryModel> summaries =
        datasetEnum.getItems().stream().map(DatasetSummary::toModel).collect(Collectors.toList());
    // Map of dataset id to list of roles.
    Map<String, List<String>> roleMap = new HashMap<>();
    for (DatasetSummary summary : datasetEnum.getItems()) {
      var roles =
          idsAndRoles.get(summary.getId()).stream()
              .map(IamRole::toString)
              .collect(Collectors.toList());
      roleMap.put(summary.getId().toString(), roles);
    }
    return new EnumerateDatasetModel()
        .items(summaries)
        .total(datasetEnum.getTotal())
        .filteredTotal(datasetEnum.getFilteredTotal())
        .roleMap(roleMap);
  }

  public String delete(String id, AuthenticatedUserRequest userReq) {
    String description = "Delete dataset " + id;
    CloudPlatform platform;
    try {
      Dataset dataset = retrieve(UUID.fromString(id));
      BillingProfileModel profileModel =
          profileDao.getBillingProfileById(dataset.getDefaultProfileId());
      platform = profileModel.getCloudPlatform();
    } catch (DatasetNotFoundException | ProfileNotFoundException e) {
      // Catching these exceptions allows for idempotent operations on delete
      // We may eventually want to change the behavior to throw on dataset not found
      platform = CloudPlatformWrapper.DEFAULT;
    }
    return jobService
        .newJob(description, DatasetDeleteFlight.class, null, userReq)
        .addParameter(JobMapKeys.DATASET_ID.getKeyName(), id)
        .addParameter(JobMapKeys.CLOUD_PLATFORM.getKeyName(), platform.name())
        .addParameter(JobMapKeys.IAM_RESOURCE_TYPE.getKeyName(), IamResourceType.DATASET)
        .addParameter(JobMapKeys.IAM_RESOURCE_ID.getKeyName(), id)
        .addParameter(JobMapKeys.IAM_ACTION.getKeyName(), IamAction.DELETE)
        .submit();
  }

  /**
   * Conditionally require sharing privileges when a caller is updating a passport identifier. Such
   * a modification indirectly affects who can access the underlying data.
   *
   * @param patchRequest updates to merge with an existing dataset
   * @return IAM actions needed to apply the requested patch
   */
  public Set<IamAction> patchDatasetIamActions(DatasetPatchRequestModel patchRequest) {
    Set<IamAction> actions = EnumSet.of(IamAction.MANAGE_SCHEMA);
    if (patchRequest.getPhsId() != null) {
      actions.add(IamAction.UPDATE_PASSPORT_IDENTIFIER);
    }
    return actions;
  }

  public DatasetSummaryModel patch(
      UUID id, DatasetPatchRequestModel patchRequest, AuthenticatedUserRequest userReq) {
    boolean patchSucceeded = datasetDao.patch(id, patchRequest, userReq);
    if (!patchSucceeded) {
      throw new RuntimeException("Dataset was not updated");
    }
    return datasetDao.retrieveSummaryById(id).toModel();
  }

  public DatasetSummaryModel setPredictableFileIds(UUID id, boolean predictableFileIds) {
    datasetDao.setPredictableFileId(id, predictableFileIds);
    return datasetDao.retrieveSummaryById(id).toModel();
  }

  public String ingestDataset(
      String id, IngestRequestModel ingestRequestModel, AuthenticatedUserRequest userReq) {
    // Fill in a default load id if the caller did not provide one in the ingest request.
    String loadTag = loadService.computeLoadTag(ingestRequestModel.getLoadTag());
    ingestRequestModel.setLoadTag(loadTag);

    Dataset dataset = null;
    // Set the profile id to the dataset's default if not specified
    if (ingestRequestModel.getProfileId() == null) {
      dataset = datasetDao.retrieve(UUID.fromString(id));
      ingestRequestModel.setProfileId(dataset.getDefaultProfileId());
    }
    String description;
    // Note: we are writing ingested to a bucket if specified before the flight starts so that the
    // data does not get materialized in the flight DB. This means that we also need to edit the
    // request to remove data that shouldn't get stored
    if (ingestRequestModel.getFormat().equals(FormatEnum.ARRAY)) {
      // get the dataset if it hasn't already been read
      if (dataset == null) {
        dataset = datasetDao.retrieve(UUID.fromString(id));
      }
      // Create staging area if needed and get the path where the temp file will live
      String tempFilePath =
          jobService
              .newJob(
                  "Create file staging area", DatasetScratchFilePrepareFlight.class, null, userReq)
              .addParameter(JobMapKeys.BILLING_ID.getKeyName(), ingestRequestModel.getProfileId())
              .addParameter(JobMapKeys.DATASET_ID.getKeyName(), id)
              .addParameter(IngestMapKeys.TABLE_NAME, ingestRequestModel.getTable())
              .addParameter(JobMapKeys.IAM_RESOURCE_TYPE.getKeyName(), IamResourceType.DATASET)
              .addParameter(JobMapKeys.IAM_RESOURCE_ID.getKeyName(), id)
              .addParameter(JobMapKeys.IAM_ACTION.getKeyName(), IamAction.INGEST_DATA)
              .submitAndWait(String.class);

      CloudPlatformWrapper cloudPlatform =
          CloudPlatformWrapper.of(dataset.getDatasetSummary().getCloudPlatform());
      String pathToUse;
      if (cloudPlatform.isGcp()) {
        pathToUse =
            writeIngestRowsToGcpBucket(dataset, tempFilePath, ingestRequestModel.getRecords());
      } else if (cloudPlatform.isAzure()) {
        pathToUse =
            writeIngestRowsToAzureStorageAccount(
                userReq,
                ingestRequestModel.getProfileId(),
                dataset,
                tempFilePath,
                ingestRequestModel.getRecords());
      } else {
        throw new IllegalArgumentException("Cloud not recognized");
      }
      ingestRequestModel.setPath(pathToUse);
      // Clear the json object so that it doesn't get written to the flight db
      ingestRequestModel.getRecords().clear();
      description =
          String.format(
              "Ingest tabular data to %s in dataset id %s", ingestRequestModel.getTable(), id);
    } else {
      description =
          String.format(
              "Ingest from %s to %s in dataset id %s",
              ingestRequestModel.getPath(), ingestRequestModel.getTable(), id);
    }

    return jobService
        .newJob(description, DatasetIngestFlight.class, ingestRequestModel, userReq)
        .addParameter(JobMapKeys.DATASET_ID.getKeyName(), id)
        .addParameter(LoadMapKeys.LOAD_TAG, loadTag)
        .addParameter(JobMapKeys.IAM_RESOURCE_TYPE.getKeyName(), IamResourceType.DATASET)
        .addParameter(JobMapKeys.IAM_RESOURCE_ID.getKeyName(), id)
        .addParameter(JobMapKeys.IAM_ACTION.getKeyName(), IamAction.INGEST_DATA)
        .submit();
  }

  public String addDatasetAssetSpecifications(
      String datasetId, AssetModel assetModel, AuthenticatedUserRequest userReq) {
    String description = "Add dataset asset specification " + assetModel.getName();
    return jobService
        .newJob(description, AddAssetSpecFlight.class, assetModel, userReq)
        .addParameter(JobMapKeys.DATASET_ID.getKeyName(), datasetId)
        .addParameter(JobMapKeys.IAM_RESOURCE_TYPE.getKeyName(), IamResourceType.DATASET)
        .addParameter(JobMapKeys.IAM_RESOURCE_ID.getKeyName(), datasetId)
        .addParameter(JobMapKeys.IAM_ACTION.getKeyName(), IamAction.MANAGE_SCHEMA)
        .submit();
  }

  public String removeDatasetAssetSpecifications(
      String datasetId, String assetName, AuthenticatedUserRequest userReq) {
    Dataset dataset = retrieve(UUID.fromString(datasetId));
    AssetSpecification asset =
        dataset
            .getAssetSpecificationByName(assetName)
            .orElseThrow(
                () ->
                    new AssetNotFoundException(
                        "This dataset does not have an asset specification with name: "
                            + assetName));
    String description = "Remove dataset asset specification " + assetName;
    String assetId = asset.getId().toString();

    return jobService
        .newJob(description, RemoveAssetSpecFlight.class, assetId, userReq)
        .addParameter(JobMapKeys.DATASET_ID.getKeyName(), datasetId)
        .addParameter(JobMapKeys.ASSET_ID.getKeyName(), assetId)
        .addParameter(JobMapKeys.IAM_RESOURCE_TYPE.getKeyName(), IamResourceType.DATASET)
        .addParameter(JobMapKeys.IAM_RESOURCE_ID.getKeyName(), datasetId)
        .addParameter(JobMapKeys.IAM_ACTION.getKeyName(), IamAction.MANAGE_SCHEMA)
        .submit();
  }

  public String deleteTabularData(
      String datasetId, DataDeletionRequest dataDeletionRequest, AuthenticatedUserRequest userReq) {
    String description = "Deleting tabular data from dataset " + datasetId;
    return jobService
        .newJob(description, DatasetDataDeleteFlight.class, dataDeletionRequest, userReq)
        .addParameter(JobMapKeys.DATASET_ID.getKeyName(), datasetId)
        .addParameter(JobMapKeys.IAM_RESOURCE_TYPE.getKeyName(), IamResourceType.DATASET)
        .addParameter(JobMapKeys.IAM_RESOURCE_ID.getKeyName(), datasetId)
        .addParameter(JobMapKeys.IAM_ACTION.getKeyName(), IamAction.SOFT_DELETE)
        .submit();
  }

  public String updateDatasetSchema(
      UUID datasetId, DatasetSchemaUpdateModel updateModel, AuthenticatedUserRequest userReq) {
    String description = "Updating dataset schema for dataset " + datasetId;
    return jobService
        .newJob(description, DatasetSchemaUpdateFlight.class, updateModel, userReq)
        .addParameter(JobMapKeys.DATASET_ID.getKeyName(), datasetId)
        .addParameter(JobMapKeys.IAM_RESOURCE_TYPE.getKeyName(), IamResourceType.DATASET)
        .addParameter(JobMapKeys.IAM_RESOURCE_ID.getKeyName(), datasetId)
        .addParameter(JobMapKeys.IAM_ACTION.getKeyName(), IamAction.MANAGE_SCHEMA)
        .submit();
  }

  public List<BulkLoadHistoryModel> getLoadHistory(
      UUID datasetId, String loadTag, int offset, int limit) {
    var dataset = retrieve(datasetId);
    var platformWrapper =
        CloudPlatformWrapper.of(dataset.getDatasetSummary().getStorageCloudPlatform());
    return platformWrapper.choose(
        Map.of(
            CloudPlatform.GCP,
            () -> bigQueryDatasetPdao.getLoadHistory(dataset, loadTag, offset, limit),
            CloudPlatform.AZURE,
            () -> storageTableService.getLoadHistory(dataset, loadTag, offset, limit)));
  }

  /**
   * @param dataset the dataset to configure the AzureDataSourceFor
   * @param userRequest the user making the request
   * @return the name of the datasource created
   * @throws RuntimeException when the external datasource could not be configured
   */
  public String getOrCreateExternalAzureDataSource(
      Dataset dataset, AuthenticatedUserRequest userRequest) {
    AccessInfoModel accessInfoModel =
        metadataDataAccessUtils.accessInfoFromDataset(dataset, userRequest);
    try {
      return azureSynapsePdao.getOrCreateExternalDataSourceForResource(
          accessInfoModel, dataset.getId(), userRequest);
    } catch (Exception e) {
      throw new RuntimeException("Could not configure external datasource", e);
    }
  }

  public DatasetDataModel retrieveData(
      AuthenticatedUserRequest userRequest,
      UUID datasetId,
      String tableName,
      int limit,
      int offset,
      String sort,
      SqlSortDirection direction,
      String filter) {
    Dataset dataset = retrieve(datasetId);

    DatasetTable table =
        dataset
            .getTableByName(tableName)
            .orElseThrow(
                () ->
                    new DatasetDataException(
                        "No dataset table exists with the name: " + tableName));

    // Assert column name provided by user is valid
    // By default sort is set to the pdao_row_id_column
    if (!sort.equalsIgnoreCase(PDAO_ROW_ID_COLUMN)) {
      table
          .getColumnByName(sort)
          .orElseThrow(
              () ->
                  new DatasetDataException(
                      "No dataset table column exists with the name: " + sort));
    }

    var cloudPlatformWrapper = CloudPlatformWrapper.of(dataset.getCloudPlatform());

    if (cloudPlatformWrapper.isGcp()) {
      try {
        List<String> columns = datasetTableDao.retrieveColumnNames(table, true);
        List<BigQueryDataResultModel> values =
            BigQueryPdao.getTable(
                dataset, tableName, columns, limit, offset, sort, direction, filter);
        return new DatasetDataModel()
            .result(
                List.copyOf(values.stream().map(BigQueryDataResultModel::getRowResult).toList()))
            .totalRowCount(
                values.isEmpty()
                    ? BigQueryPdao.getTableTotalRowCount(dataset, tableName)
                    : values.get(0).getTotalCount())
            .filteredRowCount(values.isEmpty() ? 0 : values.get(0).getFilteredCount());
      } catch (InterruptedException e) {
        throw new DatasetDataException("Error retrieving data for dataset " + dataset.getName(), e);
      }
    } else if (cloudPlatformWrapper.isAzure()) {
      String sourceParquetFilePath = IngestUtils.getSourceDatasetParquetFilePath(tableName);

      String datasourceName = getOrCreateExternalAzureDataSource(dataset, userRequest);

      List<SynapseDataResultModel> values =
          azureSynapsePdao.getTableData(
              table,
              tableName,
              datasourceName,
              sourceParquetFilePath,
              limit,
              offset,
              sort,
              direction,
              filter,
              CollectionType.DATASET);
      return new DatasetDataModel()
          .result(List.copyOf(values.stream().map(SynapseDataResultModel::getRowResult).toList()))
          .totalRowCount(
              values.isEmpty()
                  ? azureSynapsePdao.getTableTotalRowCount(
                      tableName, datasourceName, sourceParquetFilePath)
                  : values.get(0).getTotalCount())
          .filteredRowCount(values.isEmpty() ? 0 : values.get(0).getFilteredCount());
    } else {
      throw new DatasetDataException("Cloud not supported");
    }
  }

  public ColumnStatisticsModel retrieveColumnStatistics(
      AuthenticatedUserRequest userRequest,
      UUID datasetId,
      String tableName,
      String columnName,
      String filter) {
    Dataset dataset = retrieve(datasetId);

    Column column = dataset.getColumn(tableName, columnName);

    var cloudPlatformWrapper = CloudPlatformWrapper.of(dataset.getCloudPlatform());

    if (cloudPlatformWrapper.isGcp()) {
      try {
        if (column.isDoubleType()) {
          return BigQueryPdao.getStatsForDoubleColumn(dataset, tableName, column, filter);
        } else if (column.isIntType()) {
          return BigQueryPdao.getStatsForIntColumn(dataset, tableName, column, filter);
        } else if (column.isTextType()) {
          return BigQueryPdao.getStatsForTextColumn(dataset, tableName, column, filter);
        }
        return new ColumnStatisticsModel();
      } catch (InterruptedException e) {
        throw new DatasetDataException("Error retrieving data for dataset " + dataset.getName(), e);
      }
    } else if (cloudPlatformWrapper.isAzure()) {

      String sourceParquetFilePath = IngestUtils.getSourceDatasetParquetFilePath(tableName);

      String datasourceName = getOrCreateExternalAzureDataSource(dataset, userRequest);

      if (column.isDoubleType()) {
        return azureSynapsePdao.getStatsForDoubleColumn(
            column, datasourceName, sourceParquetFilePath, filter);
      } else if (column.isIntType()) {
        return azureSynapsePdao.getStatsForIntColumn(
            column, datasourceName, sourceParquetFilePath, filter);
      } else if (column.isTextType()) {
        return azureSynapsePdao.getStatsForTextColumn(
            column, datasourceName, sourceParquetFilePath, filter);
      }
      return new ColumnStatisticsModel();
    } else {
      throw new DatasetDataException("Cloud not supported");
    }
  }

  public ResourceLocks manualExclusiveLock(AuthenticatedUserRequest userReq, UUID datasetId) {
    return jobService
        .newJob(
            "Create manual exclusive lock on dataset " + datasetId,
            DatasetLockFlight.class,
            null,
            userReq)
        .addParameter(JobMapKeys.DATASET_ID.getKeyName(), datasetId)
        .submitAndWait(ResourceLocks.class);
  }

  public void lock(UUID datasetId, String flightId, boolean sharedLock) {
    if (sharedLock) {
      datasetDao.lockShared(datasetId, flightId);
    } else {
      datasetDao.lockExclusive(datasetId, flightId);
    }
  }

  public ResourceLocks manualUnlock(
      AuthenticatedUserRequest userReq, UUID datasetId, UnlockResourceRequest unlockRequest) {
    return jobService
        .newJob(
            "Remove exclusive or shared lock "
                + unlockRequest.getLockName()
                + " on dataset "
                + datasetId,
            DatasetUnlockFlight.class,
            unlockRequest,
            userReq)
        .addParameter(JobMapKeys.DATASET_ID.getKeyName(), datasetId)
        .submitAndWait(ResourceLocks.class);
  }

  public boolean unlock(UUID datasetId, String flightId, boolean sharedLock) {
    if (sharedLock) {
      return datasetDao.unlockShared(datasetId, flightId);
    }
    return datasetDao.unlockExclusive(datasetId, flightId);
  }

  public String openTransaction(
      UUID id, TransactionCreateModel transactionRequest, AuthenticatedUserRequest userReq) {
    String name;
    if (StringUtils.isEmpty(transactionRequest.getDescription())) {
      name = UUID.randomUUID().toString();
    } else {
      name = transactionRequest.getDescription();
    }
    String description = "Create transaction " + name;
    return jobService
        .newJob(description, TransactionOpenFlight.class, transactionRequest, userReq)
        .addParameter(JobMapKeys.DATASET_ID.getKeyName(), id)
        .addParameter(JobMapKeys.IAM_RESOURCE_TYPE.getKeyName(), IamResourceType.DATASET)
        .addParameter(JobMapKeys.IAM_RESOURCE_ID.getKeyName(), id)
        .addParameter(JobMapKeys.IAM_ACTION.getKeyName(), IamAction.INGEST_DATA)
        .submit();
  }

  public List<TransactionModel> enumerateTransactions(UUID id, long limit, long offset) {
    Dataset dataset = retrieve(id);
    try {
      return bigQueryTransactionPdao.enumerateTransactions(dataset, limit, offset);
    } catch (InterruptedException e) {
      throw new RuntimeException("Error enumerating transactions", e);
    }
  }

  public TransactionModel retrieveTransaction(UUID id, UUID transactionId) {
    Dataset retrieve = retrieve(id);
    try {
      return bigQueryTransactionPdao.retrieveTransaction(retrieve, transactionId);
    } catch (InterruptedException e) {
      throw new RuntimeException("Error retrieving transaction", e);
    }
  }

  public String closeTransaction(
      UUID id, UUID transactionId, AuthenticatedUserRequest userReq, ModeEnum mode) {
    switch (mode) {
      case COMMIT:
        return commitTransaction(id, transactionId, userReq);
      case ROLLBACK:
        return rollbackTransaction(id, transactionId, userReq);
      default:
        throw new IllegalArgumentException(String.format("Invalid terminal state %s", mode));
    }
  }

  private String commitTransaction(UUID id, UUID transactionId, AuthenticatedUserRequest userReq) {
    String description = "Commit transaction " + transactionId + " in dataset " + id;
    return jobService
        .newJob(description, TransactionCommitFlight.class, null, userReq)
        .addParameter(JobMapKeys.DATASET_ID.getKeyName(), id)
        .addParameter(JobMapKeys.TRANSACTION_ID.getKeyName(), transactionId)
        .addParameter(JobMapKeys.IAM_RESOURCE_TYPE.getKeyName(), IamResourceType.DATASET)
        .addParameter(JobMapKeys.IAM_RESOURCE_ID.getKeyName(), id)
        .addParameter(JobMapKeys.IAM_ACTION.getKeyName(), IamAction.INGEST_DATA)
        .submit();
  }

  private String rollbackTransaction(
      UUID id, UUID transactionId, AuthenticatedUserRequest userReq) {
    String description = "Rollback transaction " + transactionId + " in dataset " + id;
    return jobService
        .newJob(description, TransactionRollbackFlight.class, null, userReq)
        .addParameter(JobMapKeys.DATASET_ID.getKeyName(), id)
        .addParameter(JobMapKeys.TRANSACTION_ID.getKeyName(), transactionId)
        .addParameter(JobMapKeys.IAM_RESOURCE_TYPE.getKeyName(), IamResourceType.DATASET)
        .addParameter(JobMapKeys.IAM_RESOURCE_ID.getKeyName(), id)
        .addParameter(JobMapKeys.IAM_ACTION.getKeyName(), IamAction.INGEST_DATA)
        .submit();
  }

  public TagCountResultModel getTags(
      Map<UUID, Set<IamRole>> idsAndRoles, String filter, Integer limit) {
    if (idsAndRoles.isEmpty()) {
      return new TagCountResultModel().tags(List.of()).errors(List.of());
    }
    List<TagCount> tags = datasetDao.getTags(idsAndRoles.keySet(), filter, limit);
    return new TagCountResultModel().tags(tags).errors(List.of());
  }

  public DatasetSummaryModel updateTags(UUID id, TagUpdateRequestModel tagUpdateRequest) {
    boolean updateSucceeded = datasetDao.updateTags(id, tagUpdateRequest);
    if (!updateSucceeded) {
      throw new RuntimeException("Dataset tags were not updated");
    }
    return datasetDao.retrieveSummaryById(id).toModel();
  }

  private static List<DatasetRequestAccessIncludeModel> getDefaultIncludes() {
    return Arrays.stream(
            StringUtils.split(DatasetsApiController.RETRIEVE_INCLUDE_DEFAULT_VALUE, ','))
        .map(DatasetRequestAccessIncludeModel::fromValue)
        .collect(Collectors.toList());
  }

  private String writeIngestRowsToGcpBucket(
      Dataset dataset, String tempFilePath, List<Object> data) {
    try {
      String projectId = dataset.getDatasetSummary().getDataProject();

      gcsPdao.createGcsFile(tempFilePath, projectId);
      gcsPdao.writeListToCloudFile(tempFilePath, mapLines(data), projectId);
      return tempFilePath;
    } catch (IllegalArgumentException e) {
      throw new IngestFailureException("Error initializing ingest process", e);
    }
  }

  private String writeIngestRowsToAzureStorageAccount(
      AuthenticatedUserRequest userRequest,
      UUID profileId,
      Dataset dataset,
      String tempFilePath,
      List<Object> data) {
    try {
      String randomIdForFile = ShortUUID.get();
      BillingProfileModel profile = profileService.authorizeLinking(profileId, userRequest);

      AzureStorageAccountResource storageAccount =
          resourceService.getOrCreateDatasetStorageAccount(dataset, profile, randomIdForFile);

      String signedPath =
          azureBlobStorePdao.signFile(
              profile,
              storageAccount,
              tempFilePath,
              new BlobSasTokenOptions(
                  AzureBlobStorePdao.DEFAULT_SAS_TOKEN_EXPIRATION,
                  new BlobSasPermission().setReadPermission(true).setWritePermission(true),
                  userRequest.getEmail()));
      azureBlobStorePdao.writeBlobLines(signedPath, mapLines(data));
      return signedPath;
    } catch (InterruptedException e) {
      throw new IngestFailureException("Error initializing ingest process", e);
    }
  }

  private List<String> mapLines(List<Object> data) {
    return data.stream()
        .map(
            r -> {
              try {
                // We could eventually do more up front validation at this point but for
                // now depend on the ingest-into-temp-table step to fail
                return objectMapper.writeValueAsString(r);
              } catch (JsonProcessingException e) {
                throw new IllegalArgumentException("Invalid record being ingested", e);
              }
            })
        .collect(Collectors.toList());
  }
}
