package bio.terra.common.fixtures;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.CloudPlatform;
import bio.terra.model.DatasetRequestModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetUtils;
import java.io.IOException;
import java.util.UUID;
import org.springframework.stereotype.Component;

@Component
public class DaoOperations {
  private final JsonLoader jsonLoader;
  private final DatasetDao datasetDao;

  public DaoOperations(JsonLoader jsonLoader, DatasetDao datasetDao) {
    this.jsonLoader = jsonLoader;
    this.datasetDao = datasetDao;
  }

  public Dataset createMinimalDataset(
      UUID billingProfileId, UUID projectResourceId, AuthenticatedUserRequest userReq)
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
    datasetDao.createAndLock(dataset, createFlightId, userReq);
    datasetDao.unlockExclusive(dataset.getId(), createFlightId);

    return dataset;
  }
}
