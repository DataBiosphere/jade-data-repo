package bio.terra.service.dataset.flight;

import bio.terra.common.ProjectCreatingFlightKeys;

public final class DatasetWorkingMapKeys extends ProjectCreatingFlightKeys {
  private DatasetWorkingMapKeys() {}

  public static final String DATASET_ID = "datasetId";
  public static final String DATASET_NAME = "datasetName";
  public static final String ASSET_NAME_COLLISION = "assetNameCollision";
  public static final String POLICY_EMAILS = "policyEmails";
  public static final String PROJECT_RESOURCE_ID = "projectResourceId";
  public static final String PROJECT_RESOURCE_ID_LIST = "projectResourceIdList";
  public static final String PROJECTS_MARKED_FOR_DELETE = "projectsMarkedForDelete";
  public static final String APPLICATION_DEPLOYMENT_RESOURCE_ID = "applicationDeploymentResourceId";
  public static final String STORAGE_ACCOUNT_RESOURCE_ID = "storageAccountResourceId";
  public static final String SERVICE_ACCOUNT_EMAIL = "serviceAccountEmail";
  public static final String SECURE_MONITORING_ENABLED = "secureMonitoringEnabled";
}
