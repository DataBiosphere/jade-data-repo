package bio.terra.service.dataset;

import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.dataset.flight.create.DatasetCreateFlight;
import bio.terra.service.dataset.flight.delete.DatasetDeleteFlight;
import bio.terra.service.dataset.flight.ingest.DatasetIngestFlight;
import bio.terra.common.MetadataEnumeration;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.job.JobService;
import bio.terra.service.resourcemanagement.DataLocationService;
import bio.terra.model.DeleteResponseModel;
import bio.terra.model.EnumerateDatasetModel;
import bio.terra.model.IngestRequestModel;
import org.apache.commons.lang3.StringUtils;
import org.broadinstitute.dsde.workbench.client.sam.model.ResourceAndAccessPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Component
public class DatasetService {
    private static Logger logger = LoggerFactory.getLogger(DatasetService.class);

    private final DatasetDao datasetDao;
    private final JobService jobService; // for handling flight response
    private final DataLocationService dataLocationService;

    @Autowired
    public DatasetService(DatasetDao datasetDao,
                          JobService jobService,
                          DataLocationService dataLocationService) {
        this.datasetDao = datasetDao;
        this.jobService = jobService;
        this.dataLocationService = dataLocationService;
    }

    public DatasetSummaryModel createDataset(DatasetRequestModel datasetRequest, AuthenticatedUserRequest userReq) {
        String description = "Create dataset " + datasetRequest.getName();
        return jobService
            .newJob(description, DatasetCreateFlight.class, datasetRequest, userReq)
            .submitAndWait(DatasetSummaryModel.class);
    }

    public Dataset retrieve(UUID id) {
        Dataset dataset = datasetDao.retrieve(id);
        return dataset.dataProject(dataLocationService.getProjectForDataset(dataset));
    }

    public DatasetModel retrieveModel(UUID id) {
        return DatasetJsonConversion.datasetModelFromDataset(retrieve(id));
    }

    public EnumerateDatasetModel enumerate(
        int offset, int limit, String sort, String direction, String filter, List<ResourceAndAccessPolicy> resources) {
        if (resources.isEmpty()) {
            return new EnumerateDatasetModel().total(0);
        }
        List<UUID> resourceIds = resources
            .stream()
            .map(resource -> UUID.fromString(resource.getResourceId()))
            .collect(Collectors.toList());
        MetadataEnumeration<DatasetSummary> datasetEnum = datasetDao.enumerate(
            offset, limit, sort, direction, filter, resourceIds);
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
        if (StringUtils.isEmpty(ingestRequestModel.getLoadTag())) {
            String loadTag = "load-at-" + Instant.now().atZone(ZoneId.of("Z")).format(DateTimeFormatter.ISO_INSTANT);
            ingestRequestModel.setLoadTag(loadTag);
        }
        String description =
            "Ingest from " + ingestRequestModel.getPath() +
                " to " + ingestRequestModel.getTable() +
                " in dataset id " + id;
        return jobService
            .newJob(description, DatasetIngestFlight.class, ingestRequestModel, userReq)
            .addParameter(JobMapKeys.DATASET_ID.getKeyName(), id)
            .submit();
    }
}
