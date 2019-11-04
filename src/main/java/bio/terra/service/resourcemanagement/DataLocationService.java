package bio.terra.service.resourcemanagement;

import bio.terra.service.iam.sam.SamConfiguration;
import bio.terra.service.resourcemanagement.exception.DataProjectNotFoundException;
import bio.terra.service.resourcemanagement.exception.GoogleResourceNotFoundException;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetDataProject;
import bio.terra.service.dataset.DatasetDataProjectSummary;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotDataProject;
import bio.terra.service.snapshot.SnapshotDataProjectSummary;
import bio.terra.service.resourcemanagement.google.GoogleBucketRequest;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.service.resourcemanagement.google.GoogleProjectRequest;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.resourcemanagement.google.GoogleResourceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Component
public class DataLocationService {

    private static final Logger logger = LoggerFactory.getLogger(DataLocationService.class);
    // TODO: this feels like it should live in a different place, maybe GoogleResourceConfiguration?
    public static final List<String> DATA_PROJECT_SERVICE_IDS = Collections.unmodifiableList(Arrays.asList(
        "bigquery-json.googleapis.com",
        "firestore.googleapis.com",
        "firebaserules.googleapis.com",
        "storage-component.googleapis.com",
        "storage-api.googleapis.com",
        "cloudbilling.googleapis.com"
    ));
    private static final String BQ_JOB_USER_ROLE = "roles/bigquery.jobUser";

    private final DataProjectDao dataProjectDao;
    private final DataLocationSelector dataLocationSelector;
    private final GoogleResourceService resourceService;
    private final ProfileService profileService;
    private final SamConfiguration samConfiguration;

    @Autowired
    public DataLocationService(
            DataProjectDao dataProjectDao,
            DataLocationSelector dataLocationSelector,
            GoogleResourceService resourceService,
            ProfileService profileService,
            SamConfiguration samConfiguration) {
        this.dataProjectDao = dataProjectDao;
        this.dataLocationSelector = dataLocationSelector;
        this.resourceService = resourceService;
        this.profileService = profileService;
        this.samConfiguration = samConfiguration;
    }

    public Map<String, List<String>> getStewardPolicy() {
        //hard code user string for now
        String role = BQ_JOB_USER_ROLE;
        // get steward emails and add to policy
        String stewardsGroupEmail = "group:" + samConfiguration.getStewardsGroupEmail();
        Map<String, List<String>> policyMap = new HashMap<>();
        policyMap.put(role, Collections.singletonList(stewardsGroupEmail));
        return Collections.unmodifiableMap(policyMap);
    }

    public GoogleProjectResource getProjectForFile(String profileId) {
        GoogleProjectRequest googleProjectRequest = new GoogleProjectRequest()
            .projectId(dataLocationSelector.projectIdForFile(profileId))
            .profileId(UUID.fromString(profileId))
            .serviceIds(DATA_PROJECT_SERVICE_IDS);
        return resourceService.getOrCreateProject(googleProjectRequest);
    }

    public GoogleBucketResource getBucketForFile(String profileId) {
        // Every bucket needs to live in a project, so we get a project first (one will be created if it can't be found)
        GoogleProjectResource projectResource = getProjectForFile(profileId);
        BillingProfile profile = profileService.getProfileById(UUID.fromString(profileId));
        GoogleBucketRequest googleBucketRequest = new GoogleBucketRequest()
            .googleProjectResource(projectResource)
            .bucketName(dataLocationSelector.bucketForFile(profileId))
            .profileId(UUID.fromString(profileId))
            .region(profile.getGcsRegion());
        return resourceService.getOrCreateBucket(googleBucketRequest);
    }

    public GoogleBucketResource lookupBucket(String bucketResourceId) {
        return resourceService.getBucketResourceById(UUID.fromString(bucketResourceId));
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

    public DatasetDataProject getOrCreateProjectForDataset(Dataset dataset) {
        // check if for an existing DatasetDataProject first, and return here if found one
        Optional<DatasetDataProject> existingDataProject = getProjectForDataset(dataset);
        if (existingDataProject.isPresent()) {
            return existingDataProject.get();
        }

        // if we've made it here, then we need to create a new cloud resource and DatasetDataProject
        // TODO: if we are in production, don't reuse projects we don't know about
        // TODO: add a property to specify which people can view data projects
        GoogleProjectRequest googleProjectRequest = new GoogleProjectRequest()
            .projectId(dataLocationSelector.projectIdForDataset(dataset))
            .profileId(dataset.getDefaultProfileId())
            .serviceIds(DATA_PROJECT_SERVICE_IDS)
            .roleIdentityMapping(getStewardPolicy());
        GoogleProjectResource googleProjectResource = resourceService.getOrCreateProject(googleProjectRequest);

        // create the DatasetDataProjectSummary object first, which just holds all the IDs
        DatasetDataProjectSummary datasetDataProjectSummary = new DatasetDataProjectSummary()
            .projectResourceId(googleProjectResource.getRepositoryId())
            .datasetId(dataset.getId());
        UUID datasetDataProjectId = dataProjectDao.createDatasetDataProject(datasetDataProjectSummary);
        datasetDataProjectSummary.id(datasetDataProjectId);

        // then create the DatasetDataProject object from the summary
        return new DatasetDataProject(datasetDataProjectSummary).googleProjectResource(googleProjectResource);
    }

    /** Fetch existing project ID for the Dataset.
     *
     * @param dataset
     * @return a populated DatasetDataProject if one exists, empty if not
     */
    public Optional<DatasetDataProject> getProjectForDataset(Dataset dataset) {
        DatasetDataProjectSummary datasetDataProjectSummary = null;
        try {
            // first, check if DatasetDataProjectSummary (= mapping btw Dataset ID and cloud Project ID) exists
            datasetDataProjectSummary = dataProjectDao.retrieveDatasetDataProject(dataset.getId());

            // second, check if the referenced cloud resource exists
            GoogleProjectResource googleProjectResource =
                resourceService.getProjectResourceById(datasetDataProjectSummary.getProjectResourceId());

            // if both exist, then create the DatasetDataProject object from the summary and return here
            return Optional.of(
                new DatasetDataProject(datasetDataProjectSummary).googleProjectResource(googleProjectResource));
        } catch (DataProjectNotFoundException projNfEx) {
            // suppress exception here, will create later
        } catch (GoogleResourceNotFoundException rsrcNfEx) {
            // delete the bad DatasetDataProjectSummary, since its ID mapping is not valid
            // I don't think this null check will ever be false, but just in case
            if (datasetDataProjectSummary != null) {
                logger.warn("metadata has a project resource id it can't resolve for dataset: " + dataset.getName());
                dataProjectDao.deleteDatasetDataProject(datasetDataProjectSummary.getId());
            }
        }

        // did not find a valid DatasetDataProject for the given Dataset
        return Optional.empty();
    }

}
