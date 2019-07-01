package bio.terra.service;

import bio.terra.controller.AuthenticatedUserRequest;
import bio.terra.dao.DrDatasetDao;
import bio.terra.flight.dataset.create.DrDatasetCreateFlight;
import bio.terra.flight.dataset.delete.DrDatasetDeleteFlight;
import bio.terra.flight.dataset.ingest.IngestMapKeys;
import bio.terra.flight.dataset.ingest.DrDatasetIngestFlight;
import bio.terra.metadata.DrDatasetSummary;
import bio.terra.metadata.MetadataEnumeration;
import bio.terra.model.DeleteResponseModel;
import bio.terra.model.EnumerateDrDatasetModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.DrDatasetJsonConversion;
import bio.terra.model.DrDatasetModel;
import bio.terra.model.DrDatasetRequestModel;
import bio.terra.model.DrDatasetSummaryModel;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Stairway;
import org.apache.commons.lang3.StringUtils;
import org.broadinstitute.dsde.workbench.client.sam.model.ResourceAndAccessPolicy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Component
public class DrDatasetService {
    private final DrDatasetDao datasetDao;
    private final Stairway stairway;
    private final JobService jobService; // for handling flight response

    @Autowired
    public DrDatasetService(DrDatasetDao datasetDao, Stairway stairway, JobService jobService) {
        this.datasetDao = datasetDao;
        this.stairway = stairway;
        this.jobService = jobService;
    }

    public DrDatasetSummaryModel createDataset(DrDatasetRequestModel datasetRequest,
                                                 AuthenticatedUserRequest userInfo) {
        FlightMap flightMap = new FlightMap();
        flightMap.put(JobMapKeys.REQUEST.getKeyName(), datasetRequest);
        flightMap.put(JobMapKeys.DESCRIPTION.getKeyName(), "Creating a dataset");
        flightMap.put(JobMapKeys.USER_INFO.getKeyName(), userInfo);
        String flightId = stairway.submit(DrDatasetCreateFlight.class, flightMap);
        return getResponse(flightId, DrDatasetSummaryModel.class);
    }

    public DrDatasetModel retrieve(UUID id) {
        return DrDatasetJsonConversion.datasetModelFromDataset(datasetDao.retrieve(id));
    }

    public EnumerateDrDatasetModel enumerate(
        int offset, int limit, String sort, String direction, String filter, List<ResourceAndAccessPolicy> resources) {
        if (resources.isEmpty()) {
            return new EnumerateDrDatasetModel().total(0);
        }
        List<UUID> resourceIds = resources
            .stream()
            .map(resource -> UUID.fromString(resource.getResourceId()))
            .collect(Collectors.toList());
        MetadataEnumeration<DrDatasetSummary> datasetEnum = datasetDao.enumerate(
            offset, limit, sort, direction, filter, resourceIds);
        List<DrDatasetSummaryModel> summaries = datasetEnum.getItems()
            .stream()
            .map(DrDatasetJsonConversion::datasetSummaryModelFromDatasetSummary)
            .collect(Collectors.toList());
        return new EnumerateDrDatasetModel().items(summaries).total(datasetEnum.getTotal());
    }

    public DeleteResponseModel delete(UUID id, AuthenticatedUserRequest userInfo) {
        FlightMap flightMap = new FlightMap();
        flightMap.put(JobMapKeys.REQUEST.getKeyName(), id);
        flightMap.put(JobMapKeys.DESCRIPTION.getKeyName(), "Deleting the dataset with ID " + id);
        flightMap.put(JobMapKeys.USER_INFO.getKeyName(), userInfo);
        String flightId = stairway.submit(DrDatasetDeleteFlight.class, flightMap);
        return getResponse(flightId, DeleteResponseModel.class);
    }

    public String ingestDataset(String id, IngestRequestModel ingestRequestModel) {
        // Fill in a default load id if the caller did not provide one in the ingest request.
        if (StringUtils.isEmpty(ingestRequestModel.getLoadTag())) {
            String loadTag = "load-at-" + Instant.now().atZone(ZoneId.of("Z")).format(DateTimeFormatter.ISO_INSTANT);
            ingestRequestModel.setLoadTag(loadTag);
        }

        FlightMap flightMap = new FlightMap();
        flightMap.put(JobMapKeys.DESCRIPTION.getKeyName(),
            "Ingest from " + ingestRequestModel.getPath() +
                " to " + ingestRequestModel.getTable() +
                " in dataset id " + id);
        flightMap.put(JobMapKeys.REQUEST.getKeyName(), ingestRequestModel);
        flightMap.put(IngestMapKeys.DATASET_ID, id);
        return stairway.submit(DrDatasetIngestFlight.class, flightMap);
    }

    private <T> T getResponse(String flightId, Class<T> resultClass) {
        stairway.waitForFlight(flightId);
        return jobService.retrieveJobResult(flightId, resultClass);
    }
}
