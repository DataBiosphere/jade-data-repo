package bio.terra.service.dataset;

import bio.terra.common.MetadataEnumeration;
import bio.terra.model.AssetModel;
import bio.terra.model.DataDeletionRequest;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.DeleteResponseModel;
import bio.terra.model.EnumerateDatasetModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.service.dataset.flight.create.AddAssetSpecFlight;
import bio.terra.service.dataset.flight.create.DatasetCreateFlight;
import bio.terra.service.dataset.flight.datadelete.DatasetDataDeleteFlight;
import bio.terra.service.dataset.flight.delete.DatasetDeleteFlight;
import bio.terra.service.dataset.flight.delete.RemoveAssetSpecFlight;
import bio.terra.service.dataset.flight.ingest.DatasetIngestFlight;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.job.JobService;
import bio.terra.service.load.LoadService;
import bio.terra.service.resourcemanagement.DataLocationService;
import bio.terra.service.snapshot.exception.AssetNotFoundException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Component
public class DatasetService {
    private final DatasetDao datasetDao;
    private final JobService jobService; // for handling flight response
    private final DataLocationService dataLocationService;
    private final LoadService loadService;

    @Autowired
    public DatasetService(DatasetDao datasetDao,
                          JobService jobService,
                          DataLocationService dataLocationService,
                          LoadService loadService) {
        this.datasetDao = datasetDao;
        this.jobService = jobService;
        this.dataLocationService = dataLocationService;
        this.loadService = loadService;
    }

    public String createDataset(DatasetRequestModel datasetRequest, AuthenticatedUserRequest userReq) {
        String description = "Create dataset " + datasetRequest.getName();
        return jobService
            .newJob(description, DatasetCreateFlight.class, datasetRequest, userReq)
            .submit();
    }

    /** Fetch existing Dataset object.
     * @param id in UUID format
     * @return a Dataset object
     */
    public Dataset retrieve(UUID id) {
        return datasetDao.retrieve(id);
    }

    /** Convenience wrapper around fetching an existing Dataset object and converting it to a Model object.
     * Unlike the Dataset object, the Model object includes a reference to the associated cloud project.
     * @param id in UUID formant
     * @return a DatasetModel = API output-friendly representation of the Dataset
     */
    public DatasetModel retrieveModel(UUID id) {
        Dataset dataset = retrieve(id);
        DatasetDataProject dataProject = dataLocationService.getProjectOrThrow(dataset);
        return DatasetJsonConversion.populateDatasetModelFromDataset(dataset)
            .dataProject(dataProject.getGoogleProjectId());
    }

    public EnumerateDatasetModel enumerate(
        int offset, int limit, String sort, String direction, String filter, List<UUID> resources) {
        if (resources.isEmpty()) {
            return new EnumerateDatasetModel().total(0);
        }
        MetadataEnumeration<DatasetSummary> datasetEnum = datasetDao.enumerate(
            offset, limit, sort, direction, filter, resources);
        List<DatasetSummaryModel> summaries = datasetEnum.getItems()
            .stream()
            .map(DatasetJsonConversion::datasetSummaryModelFromDatasetSummary)
            .collect(Collectors.toList());
        return new EnumerateDatasetModel().items(summaries).total(datasetEnum.getTotal());
    }

    public DeleteResponseModel delete(String id, AuthenticatedUserRequest userReq) {
        String description = "Delete dataset " + id;
        return jobService
            .newJob(description, DatasetDeleteFlight.class, null, userReq)
            .addParameter(JobMapKeys.DATASET_ID.getKeyName(), id)
            .submitAndWait(DeleteResponseModel.class);
    }

    public String ingestDataset(String id, IngestRequestModel ingestRequestModel, AuthenticatedUserRequest userReq) {
        // Fill in a default load id if the caller did not provide one in the ingest request.
        String loadTag = loadService.computeLoadTag(ingestRequestModel.getLoadTag());
        ingestRequestModel.setLoadTag(loadTag);
        String description =
            "Ingest from " + ingestRequestModel.getPath() +
                " to " + ingestRequestModel.getTable() +
                " in dataset id " + id;
        return jobService
            .newJob(description, DatasetIngestFlight.class, ingestRequestModel, userReq)
            .addParameter(JobMapKeys.DATASET_ID.getKeyName(), id)
            .submit();
    }

    public String addDatasetAssetSpecifications(
        String datasetId, AssetModel assetModel, AuthenticatedUserRequest userReq
    ) {
        String description = "Add dataset asset specification " + assetModel.getName();
        return jobService
            .newJob(description, AddAssetSpecFlight.class, assetModel, userReq)
            .addParameter(JobMapKeys.DATASET_ID.getKeyName(), datasetId)
            .submit();
    }

    public String removeDatasetAssetSpecifications(
        String datasetId, String assetName, AuthenticatedUserRequest userReq
    ) {
        Dataset dataset = retrieve(UUID.fromString(datasetId));
        AssetSpecification asset = dataset
            .getAssetSpecificationByName(assetName).orElseThrow(() ->
                new AssetNotFoundException("This dataset does not have an asset specification with name: " + assetName)
            );
        String description = "Remove dataset asset specification " + assetName;
        String assetId = asset.getId().toString();

        return jobService
            .newJob(description, RemoveAssetSpecFlight.class, assetId, userReq)
            .addParameter(JobMapKeys.ASSET_ID.getKeyName(), assetId)
            .submit();
    }

    public String deleteTabularData(
        String datasetId,
        DataDeletionRequest dataDeletionRequest,
        AuthenticatedUserRequest userReq) {
        String description = "Deleting tabular data from dataset " + datasetId;
        return jobService
            .newJob(description, DatasetDataDeleteFlight.class, dataDeletionRequest, userReq)
            .addParameter(JobMapKeys.DATASET_ID.getKeyName(), datasetId)
            .submit();
    }
}
