package bio.terra.service.dataproject;

import bio.terra.dao.SnapshotDao;
import bio.terra.dao.DatasetDao;
import bio.terra.dao.exception.DataProjectNotFoundException;
import bio.terra.resourcemanagement.dao.google.GoogleResourceNotFoundException;
import bio.terra.dao.DataProjectDao;
import bio.terra.metadata.Snapshot;
import bio.terra.metadata.Dataset;
import bio.terra.metadata.SnapshotDataProject;
import bio.terra.metadata.SnapshotDataProjectSummary;
import bio.terra.resourcemanagement.metadata.google.GoogleProjectRequest;
import bio.terra.resourcemanagement.metadata.google.GoogleProjectResource;
import bio.terra.metadata.DatasetDataProject;
import bio.terra.metadata.DatasetDataProjectSummary;
import bio.terra.resourcemanagement.service.google.GoogleResourceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

@Component
public class DataProjectService {

    private static final Logger logger = LoggerFactory.getLogger(DataProjectService.class);
    private static final List<String> DATA_PROJECT_SERVICE_IDS =  Arrays.asList(
        "bigquery-json.googleapis.com",
        "firestore.googleapis.com",
        "firebaserules.googleapis.com",
        "storage-component.googleapis.com",
        "storage-api.googleapis.com",
        "cloudbilling.googleapis.com"
    );

    private final DataProjectDao dataProjectDao;
    private final DataProjectIdSelector dataProjectIdSelector;
    private final GoogleResourceService resourceService;
    private final DatasetDao datasetDao;
    private final SnapshotDao snapshotDao;

    @Autowired
    public DataProjectService(
            DataProjectDao dataProjectDao,
            DataProjectIdSelector dataProjectIdSelector,
            GoogleResourceService resourceService,
            DatasetDao datasetDao,
            SnapshotDao snapshotDao) {
        this.dataProjectDao = dataProjectDao;
        this.dataProjectIdSelector = dataProjectIdSelector;
        this.resourceService = resourceService;
        this.datasetDao = datasetDao;
        this.snapshotDao = snapshotDao;
    }

    public SnapshotDataProject getProjectForSnapshot(Snapshot snapshot) {
        SnapshotDataProjectSummary snapshotDataProjectSummary = null;
        GoogleProjectResource googleProjectResource;
        GoogleProjectRequest googleProjectRequest = new GoogleProjectRequest()
            .projectId(dataProjectIdSelector.projectIdForSnapshot(snapshot))
            .profileId(snapshot.getProfileId())
            .serviceIds(DATA_PROJECT_SERVICE_IDS);
        try {
            snapshotDataProjectSummary = dataProjectDao.retrieveSnapshotDataProjectBySnapshotId(snapshot.getId());
            googleProjectResource = resourceService.getProjectResourceById(
                snapshotDataProjectSummary.getProjectResourceId());
        } catch (DataProjectNotFoundException | GoogleResourceNotFoundException e) {
            // probably the first time we have seen this snapshot, request a new project resource and save everything
            googleProjectResource = resourceService.getOrCreateProject(googleProjectRequest);
            if (snapshotDataProjectSummary != null) {
                logger.warn("metadata has a project resource id it can't resolve for snapshot: " + snapshot.getName());
                dataProjectDao.deleteSnapshotDataProject(snapshotDataProjectSummary.getId());
            }
            snapshotDataProjectSummary = new SnapshotDataProjectSummary()
                .projectResourceId(googleProjectResource.getRepositoryId())
                .snapshotId(snapshot.getId());
            UUID snapshotDataProjectId = dataProjectDao.createSnapshotDataProject(snapshotDataProjectSummary);
            snapshotDataProjectSummary.id(snapshotDataProjectId);
        }
        return new SnapshotDataProject(snapshotDataProjectSummary)
            .googleProjectResource(googleProjectResource);
    }

    public SnapshotDataProject getProjectForSnapshotName(String snapshotName) {
        Snapshot snapshot = snapshotDao.retrieveSnapshotByName(snapshotName);
        return getProjectForSnapshot(snapshot);
    }

    // TODO: DRY this up

    public DatasetDataProject getProjectForDataset(Dataset dataset) {
        DatasetDataProjectSummary datasetDataProjectSummary = null;
        GoogleProjectResource googleProjectResource;
        GoogleProjectRequest googleProjectRequest = new GoogleProjectRequest()
            .projectId(dataProjectIdSelector.projectIdForDataset(dataset))
            .profileId(dataset.getDefaultProfileId())
            .serviceIds(DATA_PROJECT_SERVICE_IDS);
        try {
            datasetDataProjectSummary = dataProjectDao.retrieveDatasetDataProjectByDatasetId(dataset.getId());
            googleProjectResource = resourceService.getProjectResourceById(
                datasetDataProjectSummary.getProjectResourceId());
        } catch (DataProjectNotFoundException | GoogleResourceNotFoundException e) {
            // probably the first time we have seen this dataset, request a new project resource and save everything
            // TODO: if we are in production, don't reuse projects we don't know about
            // TODO: add a property to specify which people can view data projects
            googleProjectResource = resourceService.getOrCreateProject(googleProjectRequest);
            if (datasetDataProjectSummary != null) {
                logger.warn("metadata has a project resource id it can't resolve for dataset: " + dataset.getName());
                dataProjectDao.deleteDatasetDataProject(datasetDataProjectSummary.getId());
            }
            datasetDataProjectSummary = new DatasetDataProjectSummary()
                .projectResourceId(googleProjectResource.getRepositoryId())
                .datasetId(dataset.getId());
            UUID datasetDataProjectId = dataProjectDao.createDatasetDataProject(datasetDataProjectSummary);
            datasetDataProjectSummary.id(datasetDataProjectId);
        }
        return new DatasetDataProject(datasetDataProjectSummary)
            .googleProjectResource(googleProjectResource);
    }

    public DatasetDataProject getProjectForDatasetName(String datasetName) {
        Dataset dataset = datasetDao.retrieveByName(datasetName);
        return getProjectForDataset(dataset);
    }
}
