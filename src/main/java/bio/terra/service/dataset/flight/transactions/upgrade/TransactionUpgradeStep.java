package bio.terra.service.dataset.flight.transactions.upgrade;

import bio.terra.common.SqlSortDirection;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.CloudPlatform;
import bio.terra.model.EnumerateSortByParam;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDatasetPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionUpgradeStep implements Step {
  private static final Logger logger = LoggerFactory.getLogger(TransactionUpgradeStep.class);
  private final IamService iamService;
  private final DatasetService datasetService;
  private final BigQueryDatasetPdao bigQueryDatasetPdao;
  private final AuthenticatedUserRequest userReq;

  public TransactionUpgradeStep(
      IamService iamService,
      DatasetService datasetService,
      BigQueryDatasetPdao bigQueryDatasetPdao,
      AuthenticatedUserRequest userReq) {
    this.iamService = iamService;
    this.datasetService = datasetService;
    this.bigQueryDatasetPdao = bigQueryDatasetPdao;
    this.userReq = userReq;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    Map<UUID, Set<IamRole>> idsAndRoles =
        iamService.listAuthorizedResources(userReq, IamResourceType.DATASET).entrySet().stream()
            .filter(e -> e.getValue().contains(IamRole.ADMIN))
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

    List<Dataset> datasets =
        datasetService
            .enumerate(
                0,
                Integer.MAX_VALUE,
                EnumerateSortByParam.CREATED_DATE,
                SqlSortDirection.ASC,
                null,
                null,
                idsAndRoles,
                null)
            .getItems()
            .stream()
            .filter(d -> d.getCloudPlatform() == CloudPlatform.GCP)
            .map(d -> datasetService.retrieve(d.getId()))
            .collect(Collectors.toList());

    datasets.forEach(bigQueryDatasetPdao::migrateSchemaForTransactions);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    // TODO back out the insert
    return StepResult.getStepResultSuccess();
  }
}
