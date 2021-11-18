package bio.terra.service.upgrade.flight;

import bio.terra.model.EnumerateDatasetModel;
import bio.terra.model.EnumerateSortByParam;
import bio.terra.model.SqlSortDirection;
import bio.terra.model.UpgradeModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.iam.IamResourceType;
import bio.terra.service.iam.IamService;
import bio.terra.service.tabulardata.google.BigQueryProject;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.Set;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BackfillRowMetadataTablesStep implements Step {
  private static final Logger logger = LoggerFactory.getLogger(BackfillRowMetadataTablesStep.class);
  private final UpgradeModel request;
  private final DatasetService datasetService;
  private final IamService iamService;
  private final AuthenticatedUserRequest userReq;

  BackfillRowMetadataTablesStep(
      UpgradeModel request,
      DatasetService datasetService,
      IamService iamService,
      AuthenticatedUserRequest userReq) {
    this.request = request;
    this.datasetService = datasetService;
    this.iamService = iamService;
    this.userReq = userReq;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    logger.info("HELLO FROM BackfillRowMetadataTablesStep");
    // enumerate datasets
    int chunkSize = 100;
    int currentChunk = 0;
    int totalDatasetCount = 0;
    while (true) {
      Set<UUID> resources =
          iamService.listAuthorizedResources(userReq, IamResourceType.DATASET).keySet();
      EnumerateDatasetModel enumerateDatasetModel =
          datasetService.enumerate(
              currentChunk,
              chunkSize,
              EnumerateSortByParam.NAME,
              SqlSortDirection.ASC,
              null,
              null,
              resources);
      // kill forever loop when no more datasets
      int chunkDatasetCount = enumerateDatasetModel.getFilteredTotal();
      totalDatasetCount += chunkDatasetCount;
      if (chunkDatasetCount == 0) {
        logger.info("DONE - Total datasets updated: {}", totalDatasetCount);
        break;
      }
      // update chunk parameters
      currentChunk += chunkSize;

      // for each dataset:
      enumerateDatasetModel.getItems().stream()
          .forEach(
              datasetSummaryModel -> {
                // connect to big query for dataset's data project
                Dataset dataset = datasetService.retrieve(datasetSummaryModel.getId());
                BigQueryProject bigQueryProject = BigQueryProject.from(dataset);
                dataset
                    .getTables()
                    .forEach(
                        table -> {
                          // for each table:
                          // retrieve the row metadata table name from the database
                          String rowMetadataTableName = table.getRowMetadataTableName();
                          // check if row metadata table already exists in big query
                          boolean rowMetadataTableExists =
                              bigQueryProject.tableExists(dataset.getName(), rowMetadataTableName);
                          // TODO - Remove - Sanity check: Does this check find the soft delete
                          // table name?
                          boolean softDeleteTableExists =
                              bigQueryProject.tableExists(
                                  dataset.getName(), table.getSoftDeleteTableName());
                          logger.info(
                              "For dataset {}, table: {}, Found metadata table {}: {}, Found soft delete table {}: {}",
                              dataset.getName(),
                              table.getName(),
                              rowMetadataTableName,
                              rowMetadataTableExists,
                              table.getSoftDeleteTableName(),
                              softDeleteTableExists);
                          // if doesn't exist, create row metadata table

                        });
              });
    }

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
