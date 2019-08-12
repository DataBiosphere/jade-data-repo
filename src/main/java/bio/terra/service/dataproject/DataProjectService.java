package bio.terra.service.dataproject;

import bio.terra.dao.exception.DataBucketNotFoundException;
import bio.terra.dao.exception.DataProjectNotFoundException;
import bio.terra.metadata.FSFile;
import bio.terra.metadata.FileDataBucket;
import bio.terra.metadata.FileDataBucketSummary;
import bio.terra.metadata.FileDataProject;
import bio.terra.metadata.FileDataProjectSummary;
import bio.terra.resourcemanagement.dao.google.GoogleResourceNotFoundException;
import bio.terra.dao.DataProjectDao;
import bio.terra.metadata.Snapshot;
import bio.terra.metadata.Dataset;
import bio.terra.metadata.SnapshotDataProject;
import bio.terra.metadata.SnapshotDataProjectSummary;
import bio.terra.resourcemanagement.metadata.google.GoogleBucketRequest;
import bio.terra.resourcemanagement.metadata.google.GoogleBucketResource;
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
    private final DataLocationSelector dataLocationSelector;
    private final GoogleResourceService resourceService;

    @Autowired
    public DataProjectService(
            DataProjectDao dataProjectDao,
            DataLocationSelector dataLocationSelector,
            GoogleResourceService resourceService) {
        this.dataProjectDao = dataProjectDao;
        this.dataLocationSelector = dataLocationSelector;
        this.resourceService = resourceService;
    }

    public FileDataProject getProjectForFile(FSFile fsFile) {
        FileDataProjectSummary fileDataProjectSummary = null;
        GoogleProjectResource googleProjectResource;
        GoogleProjectRequest googleProjectRequest = new GoogleProjectRequest()
            .projectId(dataLocationSelector.projectIdForFile(fsFile))
            .profileId(UUID.fromString(fsFile.getProfileId()))
            .serviceIds(DATA_PROJECT_SERVICE_IDS);
        // First see if there is a saved link between a data project resource and this file's object id.
        try {
            fileDataProjectSummary = dataProjectDao.retrieveFileDataProject(fsFile.getObjectId());
            googleProjectResource = resourceService.getProjectResourceById(
                fileDataProjectSummary.getProjectResourceId());
        } catch (DataProjectNotFoundException | GoogleResourceNotFoundException e) {
            // either the project doesn't exist or we haven't seen it, so create it or get it and save it
            googleProjectResource = resourceService.getOrCreateProject(googleProjectRequest);
            if (fileDataProjectSummary != null) {
                logger.warn("metadata has a project resource id it can't resolve for file: " + fsFile.getObjectId());
                dataProjectDao.deleteFileDataProject(fileDataProjectSummary.getId());
            }
            // save a link to the google project resource for this file
            fileDataProjectSummary = new FileDataProjectSummary()
                .projectResourceId(googleProjectResource.getRepositoryId())
                .fileObjectId(fsFile.getObjectId());
            UUID fileDataProjectId = dataProjectDao.createFileDataProject(fileDataProjectSummary);
            fileDataProjectSummary.id(fileDataProjectId);
        }
        return new FileDataProject(fileDataProjectSummary)
            .googleProjectResource(googleProjectResource);
    }

    // TODO: get a project/bucket for file, how to store
    public FileDataBucket getBucketForFile(FSFile fsFile) {
        FileDataBucketSummary fileDataBucketSummary = null;
        GoogleBucketResource googleBucketResource;
        // Every bucket needs to live in a project, so we get a project first (one will be created if it can't be found)
        FileDataProject fileDataProject = getProjectForFile(fsFile);
        GoogleBucketRequest googleBucketRequest = new GoogleBucketRequest()
            .googleProjectResource(fileDataProject.getGoogleProjectResource())
            .bucketName(dataLocationSelector.bucketForFile(fsFile))
            .profileId(UUID.fromString(fsFile.getProfileId()));
        // Next see if there is a saved link between a data bucket resource and this file's object id.
        try {
            fileDataBucketSummary = dataProjectDao.retrieveFileDataBucket(fsFile.getObjectId());
            googleBucketResource = resourceService.getBucketResourceById(fileDataBucketSummary.getBucketResourceId());
        } catch (DataBucketNotFoundException | GoogleResourceNotFoundException e) {
            // either the bucket doesn't exist or we haven't seen it, so (create it or get it) and save it
            googleBucketResource = resourceService.getOrCreateBucket(googleBucketRequest);
            if (fileDataBucketSummary != null) {
                logger.warn("metadata has a bucket resource id it can't resolve for file: " + fsFile.getObjectId());
                dataProjectDao.deleteFileDataBucket(fileDataBucketSummary.getFileObjectId());
            }
            // save a link to the google bucket resource for this file
            fileDataBucketSummary = new FileDataBucketSummary()
                .bucketResourceId(googleBucketResource.getRepositoryId())
                .fileObjectId(fsFile.getObjectId());
            UUID fileDataBucketId = dataProjectDao.createFileDataBucket(fileDataBucketSummary);
            fileDataBucketSummary.id(fileDataBucketId);
        }
        return new FileDataBucket(fileDataBucketSummary)
            .googleBucketResource(googleBucketResource);
    }

    public SnapshotDataProject getProjectForSnapshot(Snapshot snapshot) {
        SnapshotDataProjectSummary snapshotDataProjectSummary = null;
        GoogleProjectResource googleProjectResource;
        GoogleProjectRequest googleProjectRequest = new GoogleProjectRequest()
            .projectId(dataLocationSelector.projectIdForSnapshot(snapshot))
            .profileId(snapshot.getProfileId())
            .serviceIds(DATA_PROJECT_SERVICE_IDS);
        try {
            snapshotDataProjectSummary = dataProjectDao.retrieveSnapshotDataProject(snapshot.getId());
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

    // TODO: DRY this up

    public DatasetDataProject getProjectForDataset(Dataset dataset) {
        DatasetDataProjectSummary datasetDataProjectSummary = null;
        GoogleProjectResource googleProjectResource;
        GoogleProjectRequest googleProjectRequest = new GoogleProjectRequest()
            .projectId(dataLocationSelector.projectIdForDataset(dataset))
            .profileId(dataset.getDefaultProfileId())
            .serviceIds(DATA_PROJECT_SERVICE_IDS);
        try {
            datasetDataProjectSummary = dataProjectDao.retrieveDatasetDataProject(dataset.getId());
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
}
