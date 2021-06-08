package bio.terra.service.resourcemanagement;

import static bio.terra.service.resourcemanagement.google.GoogleProjectService.PermissionOp.ENABLE_PERMISSIONS;
import static bio.terra.service.resourcemanagement.google.GoogleProjectService.PermissionOp.REVOKE_PERMISSIONS;

import bio.terra.app.configuration.SamConfiguration;
import bio.terra.app.model.GoogleCloudResource;
import bio.terra.app.model.GoogleRegion;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetBucketDao;
import bio.terra.service.resourcemanagement.exception.GoogleResourceNotFoundException;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.service.resourcemanagement.google.GoogleBucketService;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.resourcemanagement.google.GoogleProjectService;
import bio.terra.service.resourcemanagement.google.GoogleResourceDao;
import bio.terra.service.snapshot.exception.CorruptMetadataException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ResourceService {

    private static final Logger logger = LoggerFactory.getLogger(ResourceService.class);
    public static final String BQ_JOB_USER_ROLE = "roles/bigquery.jobUser";

    private final DataLocationSelector dataLocationSelector;
    private final GoogleProjectService projectService;
    private final GoogleBucketService bucketService;
    private final SamConfiguration samConfiguration;
    private final DatasetBucketDao datasetBucketDao;
    private final GoogleResourceDao resourceDao;


    @Autowired
    public ResourceService(
        DataLocationSelector dataLocationSelector,
        GoogleProjectService projectService,
        GoogleBucketService bucketService,
        SamConfiguration samConfiguration,
        DatasetBucketDao datasetBucketDao,
        GoogleResourceDao resourceDao) {
        this.dataLocationSelector = dataLocationSelector;
        this.projectService = projectService;
        this.bucketService = bucketService;
        this.samConfiguration = samConfiguration;
        this.datasetBucketDao = datasetBucketDao;
        this.resourceDao = resourceDao;
    }

    /**
     * Fetch/create a project, then use that to fetch/create a bucket.
     *
     * @param billingProfile authorized profile for billing account information case we need to
     *                       create a project
     * @param flightId       used to lock the bucket metadata during possible creation
     * @return a reference to the bucket as a POJO GoogleBucketResource
     * @throws CorruptMetadataException in two cases.
     * <ol>
     *     <le>if the bucket already exists, but the metadata does not AND
     *     the application property allowReuseExistingBuckets=false.</le>
     *     <le>if the metadata exists, but the bucket does not</le>
     * </ol>
     */
    public GoogleBucketResource getOrCreateBucketForFile(String possibleBucketName,
        Dataset dataset,
        BillingProfileModel billingProfile,
        String flightId) throws InterruptedException {
        final String datasetName = dataset.getName();
        final GoogleRegion region = dataset.getDatasetSummary().getStorageResourceRegion(GoogleCloudResource.FIRESTORE);
        // Every bucket needs to live in a project, so we get or create a project first
        final GoogleProjectResource projectResource = projectService.getOrCreateProject(
            dataLocationSelector.projectIdForFile(datasetName, billingProfile),
            billingProfile,
            null,
            region);

        List<UUID> bucketsForDataset = datasetBucketDao.getBucketResourceIdForDatasetId(dataset.getId());
        String bucketName = possibleBucketName;

        List<GoogleBucketResource> bucketsForBillingProfile = bucketsForDataset.stream()
            .map(this::lookupBucket)
            .filter(bucket -> bucketIsForBillingProfile(bucket, billingProfile))
            .collect(Collectors.toList());

        // there should never be more than one bucket per billing profile,
        // so we just take the first one
        if (bucketsForBillingProfile.size() > 0) {
            GoogleBucketResource bucket = bucketsForBillingProfile.get(0);
            bucketName = bucket.getName();

            if (bucketsForBillingProfile.size() > 1) {
                logger.warn("Found more than one bucket associated with this dataset and billing profile");
            }
        }

        return bucketService.getOrCreateBucket(bucketName, projectResource,
            dataset.getDatasetSummary().getStorageResourceRegion(GoogleCloudResource.BUCKET),
            flightId);
    }

    private boolean bucketIsForBillingProfile(GoogleBucketResource bucket,
        BillingProfileModel billingProfile) {
        GoogleProjectResource resource = bucket.getProjectResource();
        UUID billingProfileId = UUID.fromString(billingProfile.getId());
        return resource.getProfileId().equals(billingProfileId);
    }

    /**
     * Fetch an existing bucket and check that the associated cloud resource exists.
     *
     * @param bucketResourceId our identifier for the bucket
     * @return a reference to the bucket as a POJO GoogleBucketResource
     * @throws GoogleResourceNotFoundException if the bucket_resource metadata row does not exist
     * @throws CorruptMetadataException        if the bucket_resource metadata row exists but the
     *                                         cloud resource does not
     */
    public GoogleBucketResource lookupBucket(String bucketResourceId) {
        return lookupBucket(UUID.fromString(bucketResourceId));
    }

    public GoogleBucketResource lookupBucket(UUID bucketResourceId) {
        return bucketService.getBucketResourceById(bucketResourceId, true);
    }

    /**
     * Fetch an existing bucket_resource metadata row. Note this method does not check for the
     * existence of the underlying cloud resource. This method is intended for places where an
     * existence check on the associated cloud resource might be too much overhead (e.g. DRS
     * lookups). Most bucket lookups should use the lookupBucket method instead, which has
     * additional overhead but will catch metadata corruption errors sooner.
     *
     * @param bucketResourceId our identifier for the bucket
     * @return a reference to the bucket as a POJO GoogleBucketResource
     * @throws GoogleResourceNotFoundException if the bucket_resource metadata row does not exist
     */
    public GoogleBucketResource lookupBucketMetadata(String bucketResourceId) {
        return bucketService.getBucketResourceById(UUID.fromString(bucketResourceId), false);
    }

    /**
     * Update the bucket_resource metadata table to match the state of the underlying cloud. - If
     * the bucket exists, then the metadata row should also exist and be unlocked. - If the bucket
     * does not exist, then the metadata row should not exist. If the metadata row is locked, then
     * only the locking flight can unlock or delete the row.
     *
     * @param datasetId      id of the dataset that is storing files into the bucket
     * @param billingProfile an authorized billing profile
     * @param flightId       flight doing the updating
     */
    public void updateBucketMetadata(UUID datasetId, BillingProfileModel billingProfile,
        String flightId) {
        String bucketName = dataLocationSelector.bucketForFile(datasetId, billingProfile);
        bucketService.updateBucketMetadata(bucketName, flightId);
    }


    /**
     * Create a new project for a snapshot, if none exists already.
     *
     * @param snapshotName      name of the snapshot
     * @param billingProfile    authorized billing profile to pay for the project
     * @param firestoreRegion   the region to create the Firestore in
     * @return project resource id
     */
    public UUID getOrCreateSnapshotProject(String snapshotName,
                                           BillingProfileModel billingProfile,
                                           GoogleRegion firestoreRegion)
        throws InterruptedException {

        GoogleProjectResource googleProjectResource = projectService.getOrCreateProject(
            dataLocationSelector.projectIdForSnapshot(snapshotName, billingProfile),
            billingProfile,
            null,
            firestoreRegion);

        return googleProjectResource.getId();
    }

    /**
     * Create a new project for a dataset,  if none exists already.
     *
     * @param datasetName    name of the dataset
     * @param billingProfile authorized billing profile to pay for the project
     * @param region         the region to ceraate
     * @return project resource id
     */
    public UUID getOrCreateDatasetProject(String datasetName,
                                          BillingProfileModel billingProfile,
                                          GoogleRegion region) throws InterruptedException {

        GoogleProjectResource googleProjectResource = projectService.getOrCreateProject(
            dataLocationSelector.projectIdForDataset(datasetName, billingProfile),
            billingProfile,
            getStewardPolicy(),
            region);

        return googleProjectResource.getId();
    }

    /**
     * Look up in existing project resource given its id
     *
     * @param projectResourceId unique idea for the project
     * @return project resource
     */
    public GoogleProjectResource getProjectResource(UUID projectResourceId) {
        return projectService.getProjectResourceById(projectResourceId);
    }

    public void grantPoliciesBqJobUser(String dataProject, Collection<String> policyEmails)
        throws InterruptedException {
        final List<String> emails = policyEmails.stream().map((e) -> "group:" + e)
            .collect(Collectors.toList());
        projectService.updateIamPermissions(
            Collections.singletonMap(BQ_JOB_USER_ROLE, emails),
            dataProject,
            ENABLE_PERMISSIONS);
    }

    public void revokePoliciesBqJobUser(String dataProject, Collection<String> policyEmails)
        throws InterruptedException {
        final List<String> emails = policyEmails.stream().map((e) -> "group:" + e)
            .collect(Collectors.toList());
        projectService.updateIamPermissions(
            Collections.singletonMap(BQ_JOB_USER_ROLE, emails),
            dataProject,
            REVOKE_PERMISSIONS);
    }


    private Map<String, List<String>> getStewardPolicy() {
        // get steward emails and add to policy
        String stewardsGroupEmail = "group:" + samConfiguration.getStewardsGroupEmail();
        Map<String, List<String>> policyMap = new HashMap<>();
        policyMap.put(BQ_JOB_USER_ROLE, Collections.singletonList(stewardsGroupEmail));
        return Collections.unmodifiableMap(policyMap);
    }

    public List<UUID> markUnusedProjectsForDelete(String profileId) {
        return projectService.markUnusedProjectsForDelete(profileId);
    }

    public void deleteUnusedProjects(List<UUID> projectIdList) {
        projectService.deleteUnusedProjects(projectIdList);
    }

    public void deleteProjectMetadata(List<UUID> projectIdList) {
        projectService.deleteProjectMetadata(projectIdList);
    }

}
