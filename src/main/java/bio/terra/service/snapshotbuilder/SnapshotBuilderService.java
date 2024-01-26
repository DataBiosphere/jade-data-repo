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
import bio.terra.model.SnapshotBuilderGetConceptsResponse;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.snapshotbuilder.query.TableNameGenerator;
import bio.terra.service.snapshotbuilder.utils.AggregateBQQueryResultsUtils;
import bio.terra.service.snapshotbuilder.utils.AggregateSynapseQueryResultsUtils;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDatasetPdao;
import bio.terra.service.tabulardata.google.bigquery.BigQueryPdao;
import com.google.cloud.bigquery.TableResult;
import java.sql.ResultSet;
import bio.terra.service.snapshotbuilder.query.FieldPointer;
import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.FilterVariable;
import bio.terra.service.snapshotbuilder.query.Literal;
import bio.terra.service.snapshotbuilder.query.Query;
import bio.terra.service.snapshotbuilder.query.TablePointer;
import bio.terra.service.snapshotbuilder.query.TableVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BinaryFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BooleanAndOrFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.FunctionFilterVariable;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import org.apache.commons.lang3.NotImplementedException;
import org.springframework.stereotype.Component;

@Component
public class SnapshotBuilderService {

  private final SnapshotRequestDao snapshotRequestDao;
  private final DatasetService datasetService;
  private final BigQueryDatasetPdao bigQueryDatasetPdao;

  private final AzureSynapsePdao azureSynapsePdao;

  public SnapshotBuilderService(
      SnapshotRequestDao snapshotRequestDao,
      DatasetService datasetService,
      BigQueryDatasetPdao bigQueryDatasetPdao,
      AzureSynapsePdao azureSynapsePdao) {
    this.snapshotRequestDao = snapshotRequestDao;
    this.datasetService = datasetService;
    this.bigQueryDatasetPdao = bigQueryDatasetPdao;
    this.azureSynapsePdao = azureSynapsePdao;
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
      throw new NotImplementedException("Cloud platform not implemented");
    }
  }

  private <T> List<T> runSnapshotBuilderQuery(
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
      throw new NotImplementedException("Cloud platform not implemented");
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

  private int getRollupCount(UUID datasetId, List<SnapshotBuilderCohort> cohorts) {
    return 100;
  }

  public SnapshotBuilderCountResponse getCountResponse(
      UUID id, List<SnapshotBuilderCohort> cohorts) {
    return new SnapshotBuilderCountResponse()
        .sql("")
        .result(new SnapshotBuilderCountResponseResult().total(getRollupCount(id, cohorts)));
  }

  public EnumerateSnapshotAccessRequest enumerateByDatasetId(UUID id) {
    return convertToEnumerateModel(snapshotRequestDao.enumerateByDatasetId(id));
  }

  public SnapshotBuilderGetConceptsResponse searchConcepts(
      UUID datasetId, String domainId, String searchText, AuthenticatedUserRequest userRequest) {
    Dataset dataset = datasetService.retrieve(datasetId);
    TableNameGenerator tableNameGenerator =
        switch (dataset.getCloudPlatform()) {
          case GCP -> BigQueryVisitor.bqTableName(
              datasetService.retrieveDatasetModel(datasetId, userRequest));
          case AZURE -> SynapseVisitor.azureTableName(
              datasetService.getOrCreateExternalAzureDataSource(dataset, userRequest));
        };
    String cloudSpecificSql = buildSearchConceptsQuery(datasetId, domainId, searchText, tableNameGenerator);
    List<SnapshotBuilderConcept> concepts =
        runSnapshotBuilderQuery(
            cloudSpecificSql,
            dataset,
            AggregateBQQueryResultsUtils::aggregateConceptResults,
            AggregateSynapseQueryResultsUtils::aggregateConceptResult);
    return new SnapshotBuilderGetConceptsResponse().result(concepts);
  }

  private String buildSearchConceptsQuery(UUID datasetId, String domainId, String searchText,
        TableNameGenerator tableNameGenerator) {
    TablePointer conceptTablePointer = TablePointer.fromTableName("concept", tableNameGenerator);
    TableVariable conceptTableVariable = TableVariable.forPrimary(conceptTablePointer);
    FieldPointer nameFieldPointer = new FieldPointer(conceptTablePointer, "concept_name");
    FieldVariable nameFieldVariable = new FieldVariable(nameFieldPointer, conceptTableVariable);
    FieldPointer idFieldPointer = new FieldPointer(conceptTablePointer, "concept_id");
    FieldVariable idFieldVariable = new FieldVariable(idFieldPointer, conceptTableVariable);

    BinaryFilterVariable domainClause =
        new BinaryFilterVariable(
            new FieldVariable(
                new FieldPointer(conceptTablePointer, "domain_id"), conceptTableVariable),
            BinaryFilterVariable.BinaryOperator.EQUALS,
            new Literal(domainId));
    FunctionFilterVariable searchConceptNameClause =
        new FunctionFilterVariable(
            FunctionFilterVariable.FunctionTemplate.TEXT_EXACT_MATCH,
            new FieldVariable(
                new FieldPointer(conceptTablePointer, "concept_name"), conceptTableVariable),
            new Literal(searchText));
    FunctionFilterVariable searchConceptCodeClause =
        new FunctionFilterVariable(
            FunctionFilterVariable.FunctionTemplate.TEXT_EXACT_MATCH,
            new FieldVariable(
                new FieldPointer(conceptTablePointer, "concept_code"), conceptTableVariable),
            new Literal(searchText));
    List<FilterVariable> searches = List.of(searchConceptNameClause, searchConceptCodeClause);
    BooleanAndOrFilterVariable searchClause =
        new BooleanAndOrFilterVariable(BooleanAndOrFilterVariable.LogicalOperator.OR, searches);
    List<FilterVariable> allFilters = List.of(domainClause, searchClause);
    BooleanAndOrFilterVariable whereClause =
        new BooleanAndOrFilterVariable(BooleanAndOrFilterVariable.LogicalOperator.AND, allFilters);

    Query query =
        new Query(
            List.of(nameFieldVariable, idFieldVariable),
            List.of(conceptTableVariable),
            whereClause);
    return query.renderSQL();
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
