package bio.terra.service.dataset;

import bio.terra.app.controller.DatasetsApiController;
import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.exception.InvalidCloudPlatformException;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.AssetModel;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BulkLoadHistoryModel;
import bio.terra.model.CloudPlatform;
import bio.terra.model.DataDeletionRequest;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetRequestAccessIncludeModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.EnumerateDatasetModel;
import bio.terra.model.EnumerateSortByParam;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.SqlSortDirection;
import bio.terra.model.TransactionCloseModel.ModeEnum;
import bio.terra.model.TransactionCreateModel;
import bio.terra.model.TransactionModel;
import bio.terra.service.dataset.exception.DatasetNotFoundException;
import bio.terra.service.dataset.flight.create.AddAssetSpecFlight;
import bio.terra.service.dataset.flight.create.DatasetCreateFlight;
import bio.terra.service.dataset.flight.datadelete.DatasetDataDeleteFlight;
import bio.terra.service.dataset.flight.delete.DatasetDeleteFlight;
import bio.terra.service.dataset.flight.delete.RemoveAssetSpecFlight;
import bio.terra.service.dataset.flight.ingest.DatasetIngestFlight;
import bio.terra.service.dataset.flight.transactions.TransactionCommitFlight;
import bio.terra.service.dataset.flight.transactions.TransactionOpenFlight;
import bio.terra.service.dataset.flight.transactions.TransactionRollbackFlight;
import bio.terra.service.iam.IamRole;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.job.JobService;
import bio.terra.service.load.LoadService;
import bio.terra.service.load.flight.LoadMapKeys;
import bio.terra.service.profile.ProfileDao;
import bio.terra.service.profile.exception.ProfileNotFoundException;
import bio.terra.service.resourcemanagement.MetadataDataAccessUtils;
import bio.terra.service.snapshot.exception.AssetNotFoundException;
import bio.terra.service.tabulardata.azure.StorageTableService;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class DatasetService {

  private final DatasetDao datasetDao;
  private final JobService jobService; // for handling flight response
  private final LoadService loadService;
  private final ProfileDao profileDao;
  private final StorageTableService storageTableService;
  private final BigQueryPdao bigQueryPdao;
  private final MetadataDataAccessUtils metadataDataAccessUtils;

  @Autowired
  public DatasetService(
      DatasetDao datasetDao,
      JobService jobService,
      LoadService loadService,
      ProfileDao profileDao,
      StorageTableService storageTableService,
      BigQueryPdao bigQueryPdao,
      MetadataDataAccessUtils metadataDataAccessUtils) {
    this.datasetDao = datasetDao;
    this.jobService = jobService;
    this.loadService = loadService;
    this.profileDao = profileDao;
    this.storageTableService = storageTableService;
    this.bigQueryPdao = bigQueryPdao;
    this.metadataDataAccessUtils = metadataDataAccessUtils;
  }

  public String createDataset(
      DatasetRequestModel datasetRequest, AuthenticatedUserRequest userReq) {
    String description = "Create dataset " + datasetRequest.getName();
    return jobService
        .newJob(description, DatasetCreateFlight.class, datasetRequest, userReq)
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
   * Fetch existing Dataset object that is NOT exclusively locked.
   *
   * @param id in UUID format
   * @return a Dataset object
   */
  public Dataset retrieveAvailable(UUID id) {
    return datasetDao.retrieveAvailable(id);
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
   * <p>Note that this method will only return a dataset if it is NOT exclusively locked. It is
   * intended for user-facing calls (e.g. from RepositoryApiController), not internal calls that may
   * require an exclusively locked dataset to be returned (e.g. dataset deletion).
   *
   * @param id in UUID format
   * @return a DatasetModel = API output-friendly representation of the Dataset
   */
  public DatasetModel retrieveAvailableDatasetModel(
      UUID id,
      AuthenticatedUserRequest userRequest,
      List<DatasetRequestAccessIncludeModel> include) {
    Dataset dataset = retrieveAvailable(id);
    return retrieveModel(dataset, userRequest, include);
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
    return DatasetJsonConversion.populateDatasetModelFromDataset(
        dataset, include, metadataDataAccessUtils, userRequest);
  }

  public EnumerateDatasetModel enumerate(
      int offset,
      int limit,
      EnumerateSortByParam sort,
      SqlSortDirection direction,
      String filter,
      String region,
      Map<UUID, Set<IamRole>> idsAndRoles) {
    if (idsAndRoles.isEmpty()) {
      return new EnumerateDatasetModel().total(0).items(List.of());
    }
    var datasetEnum =
        datasetDao.enumerate(offset, limit, sort, direction, filter, region, idsAndRoles.keySet());

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
        .submit();
  }

  public String ingestDataset(
      String id, IngestRequestModel ingestRequestModel, AuthenticatedUserRequest userReq) {
    // Fill in a default load id if the caller did not provide one in the ingest request.
    String loadTag = loadService.computeLoadTag(ingestRequestModel.getLoadTag());
    ingestRequestModel.setLoadTag(loadTag);
    String description =
        "Ingest from "
            + ingestRequestModel.getPath()
            + " to "
            + ingestRequestModel.getTable()
            + " in dataset id "
            + id;
    return jobService
        .newJob(description, DatasetIngestFlight.class, ingestRequestModel, userReq)
        .addParameter(JobMapKeys.DATASET_ID.getKeyName(), id)
        .addParameter(LoadMapKeys.LOAD_TAG, loadTag)
        .submit();
  }

  public String addDatasetAssetSpecifications(
      String datasetId, AssetModel assetModel, AuthenticatedUserRequest userReq) {
    String description = "Add dataset asset specification " + assetModel.getName();
    return jobService
        .newJob(description, AddAssetSpecFlight.class, assetModel, userReq)
        .addParameter(JobMapKeys.DATASET_ID.getKeyName(), datasetId)
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
        .addParameter(JobMapKeys.ASSET_ID.getKeyName(), assetId)
        .submit();
  }

  public String deleteTabularData(
      String datasetId, DataDeletionRequest dataDeletionRequest, AuthenticatedUserRequest userReq) {
    String description = "Deleting tabular data from dataset " + datasetId;
    return jobService
        .newJob(description, DatasetDataDeleteFlight.class, dataDeletionRequest, userReq)
        .addParameter(JobMapKeys.DATASET_ID.getKeyName(), datasetId)
        .submit();
  }

  public List<BulkLoadHistoryModel> getLoadHistory(
      UUID datasetId, String loadTag, int offset, int limit) {
    var dataset = retrieve(datasetId);
    var platformWrapper =
        CloudPlatformWrapper.of(dataset.getDatasetSummary().getStorageCloudPlatform());
    if (platformWrapper.isAzure()) {
      return storageTableService.getLoadHistory(dataset, loadTag, offset, limit);
    } else if (platformWrapper.isGcp()) {
      return bigQueryPdao.getLoadHistory(dataset, loadTag, offset, limit);
    } else {
      throw new InvalidCloudPlatformException();
    }
  }

  public void lock(UUID datasetId, String flightId, boolean sharedLock) {
    if (sharedLock) {
      datasetDao.lockShared(datasetId, flightId);
    } else {
      datasetDao.lockExclusive(datasetId, flightId);
    }
  }

  public void unlock(UUID datasetId, String flightId, boolean sharedLock) {
    if (sharedLock) {
      datasetDao.unlockShared(datasetId, flightId);
    } else {
      datasetDao.unlockExclusive(datasetId, flightId);
    }
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
        .submit();
  }

  public List<TransactionModel> enumerateTransactions(UUID id, long limit, long offset) {
    Dataset dataset = retrieve(id);
    try {
      return bigQueryPdao.enumerateTransactions(dataset, limit, offset);
    } catch (InterruptedException e) {
      throw new RuntimeException("Error enumerating transactions", e);
    }
  }

  public TransactionModel retrieveTransaction(UUID id, UUID transactionId) {
    Dataset retrieve = retrieve(id);
    try {
      return bigQueryPdao.retrieveTransaction(retrieve, transactionId);
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
        .submit();
  }

  private String rollbackTransaction(
      UUID id, UUID transactionId, AuthenticatedUserRequest userReq) {
    String description = "Rollback transaction " + transactionId + " in dataset " + id;
    return jobService
        .newJob(description, TransactionRollbackFlight.class, null, userReq)
        .addParameter(JobMapKeys.DATASET_ID.getKeyName(), id)
        .addParameter(JobMapKeys.TRANSACTION_ID.getKeyName(), transactionId)
        .submit();
  }

  private static List<DatasetRequestAccessIncludeModel> getDefaultIncludes() {
    return Arrays.stream(
            StringUtils.split(DatasetsApiController.RETRIEVE_INCLUDE_DEFAULT_VALUE, ','))
        .map(DatasetRequestAccessIncludeModel::fromValue)
        .collect(Collectors.toList());
  }
}
