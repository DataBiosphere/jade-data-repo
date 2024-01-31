package bio.terra.service.snapshotbuilder;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.grammar.azure.SynapseVisitor;
import bio.terra.grammar.google.BigQueryVisitor;
import bio.terra.model.EnumerateSnapshotAccessRequest;
import bio.terra.model.EnumerateSnapshotAccessRequestItem;
import bio.terra.model.SnapshotAccessRequest;
import bio.terra.model.SnapshotAccessRequestResponse;
import bio.terra.model.SnapshotBuilderCohort;
import bio.terra.model.SnapshotBuilderConcept;
import bio.terra.model.SnapshotBuilderCountResponse;
import bio.terra.model.SnapshotBuilderCountResponseResult;
import bio.terra.model.SnapshotBuilderCriteriaGroup;
import bio.terra.model.SnapshotBuilderGetConceptsResponse;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.snapshotbuilder.query.Query;
import bio.terra.service.snapshotbuilder.query.TableNameGenerator;
import bio.terra.service.snapshotbuilder.utils.AggregateBQQueryResultsUtils;
import bio.terra.service.snapshotbuilder.utils.AggregateSynapseQueryResultsUtils;
import bio.terra.service.snapshotbuilder.utils.CriteriaQueryBuilderFactory;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDatasetPdao;
import bio.terra.service.tabulardata.google.bigquery.BigQueryPdao;
import com.google.cloud.bigquery.TableResult;
import com.google.common.annotations.VisibleForTesting;
import java.sql.ResultSet;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import org.apache.commons.lang3.NotImplementedException;
import org.springframework.stereotype.Component;

@Component
public class SnapshotBuilderService {

  public static final String CLOUD_PLATFORM_NOT_IMPLEMENTED_MESSAGE =
      "Cloud platform not implemented";
  private final SnapshotRequestDao snapshotRequestDao;
  private final DatasetService datasetService;
  private final BigQueryDatasetPdao bigQueryDatasetPdao;

  private final AzureSynapsePdao azureSynapsePdao;
  private final CriteriaQueryBuilderFactory criteriaQueryBuilderFactory;

  public SnapshotBuilderService(
      SnapshotRequestDao snapshotRequestDao,
      DatasetService datasetService,
      BigQueryDatasetPdao bigQueryDatasetPdao,
      AzureSynapsePdao azureSynapsePdao,
      CriteriaQueryBuilderFactory criteriaQueryBuilderFactory) {
    this.snapshotRequestDao = snapshotRequestDao;
    this.datasetService = datasetService;
    this.bigQueryDatasetPdao = bigQueryDatasetPdao;
    this.azureSynapsePdao = azureSynapsePdao;
    this.criteriaQueryBuilderFactory = criteriaQueryBuilderFactory;
  }

  public SnapshotAccessRequestResponse createSnapshotRequest(
      UUID id, SnapshotAccessRequest snapshotAccessRequest, String email) {
    return snapshotRequestDao.create(id, snapshotAccessRequest, email);
  }

  // TODO - replace manual query with query builder
  // The query builder will handle switching between cloud platforms, so we won't
  // have this duplicated logic in the final solution.
  private String buildConceptChildrenQuery(
      Dataset dataset, int conceptId, AuthenticatedUserRequest userRequest) {
    var cloudPlatformWrapper = CloudPlatformWrapper.of(dataset.getCloudPlatform());
    if (cloudPlatformWrapper.isGcp()) {
      var bqTablePrefix =
          String.format(
              "%s.%s",
              dataset.getProjectResource().getGoogleProjectId(),
              BigQueryPdao.prefixName(dataset.getName()));

      return """
                  SELECT c.concept_name, c.concept_id FROM `%s.concept` AS c
                  WHERE c.concept_id IN
                  (SELECT c.descendant_concept_id FROM `%s.concept_ancestor` AS c
                  WHERE c.ancestor_concept_id = %d)
          """
          .formatted(bqTablePrefix, bqTablePrefix, conceptId);
    } else if (cloudPlatformWrapper.isAzure()) {
      String conceptParquetFilePath = IngestUtils.getSourceDatasetParquetFilePath("concept");
      String conceptAncestorFilePath =
          IngestUtils.getSourceDatasetParquetFilePath("concept_ancestor");

      String datasourceName =
          datasetService.getOrCreateExternalAzureDataSource(dataset, userRequest);
      return """
            SELECT c.concept_name, c.concept_id FROM
            (SELECT * FROM OPENROWSET(BULK '%s', DATA_SOURCE = '%s', FORMAT = 'parquet') AS alias951024263)
            AS c WHERE c.concept_id IN
            (SELECT c.descendant_concept_id FROM
            (SELECT * FROM OPENROWSET(BULK '%s', DATA_SOURCE = '%s', FORMAT = 'parquet') AS alias625571305)
            AS c WHERE c.ancestor_concept_id = %d)
          """
          .formatted(
              conceptParquetFilePath,
              datasourceName,
              conceptAncestorFilePath,
              datasourceName,
              conceptId);

    } else {
      throw new NotImplementedException(CLOUD_PLATFORM_NOT_IMPLEMENTED_MESSAGE);
    }
  }

  public <T> List<T> runSnapshotBuilderQuery(
      String cloudSpecificSql,
      Dataset dataset,
      Function<TableResult, List<T>> gcpFormatQueryFunction,
      Function<ResultSet, T> synapseFormatQueryFunction) {
    var cloudPlatformWrapper = CloudPlatformWrapper.of(dataset.getCloudPlatform());
    if (cloudPlatformWrapper.isGcp()) {
      return bigQueryDatasetPdao.runQuery(cloudSpecificSql, dataset, gcpFormatQueryFunction);
    } else if (cloudPlatformWrapper.isAzure()) {
      return azureSynapsePdao.runQuery(cloudSpecificSql, synapseFormatQueryFunction);
    } else {
      throw new NotImplementedException(CLOUD_PLATFORM_NOT_IMPLEMENTED_MESSAGE);
    }
  }

  public SnapshotBuilderGetConceptsResponse getConceptChildren(
      UUID datasetId, Integer conceptId, AuthenticatedUserRequest userRequest) {
    // TODO: Build real query - this should get the name and ID from the concept table, the count
    // from the occurrence table, and the existence of children from the concept_ancestor table.
    Dataset dataset = datasetService.retrieve(datasetId);

    String cloudSpecificSQL = buildConceptChildrenQuery(dataset, conceptId, userRequest);

    List<SnapshotBuilderConcept> concepts =
        runSnapshotBuilderQuery(
            cloudSpecificSQL,
            dataset,
            AggregateBQQueryResultsUtils::aggregateConceptResults,
            AggregateSynapseQueryResultsUtils::aggregateConceptResult);
    return new SnapshotBuilderGetConceptsResponse().result(concepts);
  }

  public SnapshotBuilderCountResponse getCountResponse(
      UUID id, List<SnapshotBuilderCohort> cohorts, AuthenticatedUserRequest userRequest) {
    int rollupCount =
        getRollupCountForCriteriaGroups(
            id,
            cohorts.stream().map(SnapshotBuilderCohort::getCriteriaGroups).toList(),
            userRequest);
    return new SnapshotBuilderCountResponse()
        .result(new SnapshotBuilderCountResponseResult().total(Math.max(rollupCount, 20)));
  }

  public EnumerateSnapshotAccessRequest enumerateByDatasetId(UUID id) {
    return convertToEnumerateModel(snapshotRequestDao.enumerateByDatasetId(id));
  }

  public int getRollupCountForCriteriaGroups(
      UUID datasetId,
      List<List<SnapshotBuilderCriteriaGroup>> criteriaGroups,
      AuthenticatedUserRequest userRequest) {
    Dataset dataset = datasetService.retrieve(datasetId);
    TableNameGenerator tableNameGenerator = getTableNameGenerator(userRequest, dataset);

    Query query =
        criteriaQueryBuilderFactory
            .createCriteriaQueryBuilder("person", tableNameGenerator)
            .generateRollupCountsQueryForCriteriaGroupsList(criteriaGroups);
    String cloudSpecificSQL = query.renderSQL();

    return runSnapshotBuilderQuery(
            cloudSpecificSQL,
            dataset,
            AggregateBQQueryResultsUtils::rollupCountsMapper,
            AggregateSynapseQueryResultsUtils::rollupCountsMapper)
        .get(0);
  }

  @VisibleForTesting
  public TableNameGenerator getTableNameGenerator(
      AuthenticatedUserRequest userRequest, Dataset dataset) {
    CloudPlatformWrapper cloudPlatformWrapper = CloudPlatformWrapper.of(dataset.getCloudPlatform());
    if (cloudPlatformWrapper.isAzure()) {
      return SynapseVisitor.azureTableName(
          datasetService.getOrCreateExternalAzureDataSource(dataset, userRequest));
    } else if (cloudPlatformWrapper.isGcp()) {
      return BigQueryVisitor.bqTableName(
          datasetService.retrieveDatasetModel(dataset.getId(), userRequest));
    } else {
      throw new NotImplementedException(CLOUD_PLATFORM_NOT_IMPLEMENTED_MESSAGE);
    }
  }

  private EnumerateSnapshotAccessRequest convertToEnumerateModel(
      List<SnapshotAccessRequestResponse> responses) {
    EnumerateSnapshotAccessRequest enumerateModel = new EnumerateSnapshotAccessRequest();
    for (SnapshotAccessRequestResponse response : responses) {
      enumerateModel.addItemsItem(
          new EnumerateSnapshotAccessRequestItem()
              .id(response.getId())
              .status(response.getStatus())
              .createdDate(response.getCreatedDate())
              .name(response.getSnapshotName())
              .researchPurpose(response.getSnapshotResearchPurpose())
              .createdBy(response.getCreatedBy()));
    }
    return enumerateModel;
  }
}
