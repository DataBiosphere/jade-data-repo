package bio.terra.service.upgrade.flight;

import static bio.terra.service.tabulardata.google.BigQueryPdao.prefixName;

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
    // Appears the max in an environment is 188 in dev
    int limit = 1000;
    int skip = 0;
    int totalDatasetCount = 0;
    Set<UUID> resources =
        iamService.listAuthorizedResources(userReq, IamResourceType.DATASET).keySet();
    EnumerateDatasetModel enumerateDatasetModel =
        datasetService.enumerate(
            skip, limit, EnumerateSortByParam.NAME, SqlSortDirection.ASC, null, null, resources);
    int chunkDatasetCount = enumerateDatasetModel.getFilteredTotal();
    totalDatasetCount += chunkDatasetCount;

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
                            bigQueryProject.tableExists(
                                prefixName(dataset.getName()), rowMetadataTableName);
                        // TODO - Remove - Sanity check: Does this check find the soft delete
                        // table name?
                        boolean softDeleteTableExists =
                            bigQueryProject.tableExists(
                                prefixName(dataset.getName()), table.getSoftDeleteTableName());
                        logger.info(
                            "For dataset {}, table: {}, Found metadata table {}, Found soft delete table {}",
                            dataset.getId(),
                            table.getName(),
                            rowMetadataTableExists,
                            softDeleteTableExists);
                        // if doesn't exist, create row metadata table

                      });
            });
    logger.info("DONE - Total datasets updated: {}", totalDatasetCount);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
