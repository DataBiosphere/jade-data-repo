package bio.terra.common.fixtures;

import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.model.CloudPlatform;
import bio.terra.model.DatasetRequestModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetUtils;
import bio.terra.service.profile.ProfileDao;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.resourcemanagement.google.GoogleResourceDao;
import bio.terra.service.snapshot.Snapshot;
import java.io.IOException;
import java.util.UUID;
import org.springframework.stereotype.Component;

@Component
public class DaoOperations {
  private final JsonLoader jsonLoader;
  private final DatasetDao datasetDao;
  private final ProfileDao profileDao;
  private final GoogleResourceDao resourceDao;

  public DaoOperations(
      JsonLoader jsonLoader,
      DatasetDao datasetDao,
      ProfileDao profileDao,
      GoogleResourceDao resourceDao) {
    this.jsonLoader = jsonLoader;
    this.datasetDao = datasetDao;
    this.profileDao = profileDao;
    this.resourceDao = resourceDao;
  }

  public Dataset createMinimalDataset() throws IOException {
    BillingProfileRequestModel profileRequest = ProfileFixtures.randomBillingProfileRequest();
    BillingProfileModel billingProfile =
        profileDao.createBillingProfile(profileRequest, "testUser");

    GoogleProjectResource projectResource = ResourceFixtures.randomProjectResource(billingProfile);
    UUID projectId = resourceDao.createProject(projectResource);
    projectResource.id(projectId);

    return createMinimalDataset(billingProfile.getId(), projectId);
  }

  public Snapshot createMinimalSnapshot() {
    UUID snapshotId = UUID.randomUUID();
    return new Snapshot().id(snapshotId);
  }

  public Dataset createMinimalDataset(UUID billingProfileId, UUID projectResourceId)
      throws IOException {
    DatasetRequestModel datasetRequest =
        jsonLoader.loadObject("dataset-minimal.json", DatasetRequestModel.class);
    String newName = datasetRequest.getName() + UUID.randomUUID();
    datasetRequest
        .name(newName)
        .defaultProfileId(billingProfileId)
        .cloudPlatform(CloudPlatform.GCP);
    Dataset dataset = DatasetUtils.convertRequestWithGeneratedNames(datasetRequest);
    dataset.projectResourceId(projectResourceId);
    String createFlightId = UUID.randomUUID().toString();
    UUID datasetId = UUID.randomUUID();
    dataset.id(datasetId);
    datasetDao.createAndLock(dataset, createFlightId);
    datasetDao.unlockExclusive(dataset.getId(), createFlightId);

    return dataset;
  }
}
