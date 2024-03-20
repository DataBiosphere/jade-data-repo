package bio.terra.service.snapshotbuilder;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.exception.BadRequestException;
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
import bio.terra.model.SnapshotBuilderDomainOption;
import bio.terra.model.SnapshotBuilderGetConceptHierarchyResponse;
import bio.terra.model.SnapshotBuilderGetConceptsResponse;
import bio.terra.model.SnapshotBuilderParentConcept;
import bio.terra.model.SnapshotBuilderSettings;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.filedata.exception.ProcessResultSetException;
import bio.terra.service.snapshotbuilder.query.Query;
import bio.terra.service.snapshotbuilder.query.TableNameGenerator;
import bio.terra.service.snapshotbuilder.utils.AggregateBQQueryResultsUtils;
import bio.terra.service.snapshotbuilder.utils.AggregateSynapseQueryResultsUtils;
import bio.terra.service.snapshotbuilder.utils.ConceptChildrenQueryBuilder;
import bio.terra.service.snapshotbuilder.utils.HierarchyQueryBuilder;
import bio.terra.service.snapshotbuilder.utils.QueryBuilderFactory;
import bio.terra.service.snapshotbuilder.utils.SearchConceptsQueryBuilder;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDatasetPdao;
import com.google.cloud.bigquery.TableResult;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.commons.lang3.NotImplementedException;
import org.springframework.stereotype.Component;

@Component
public class SnapshotBuilderService {

  public static final String CLOUD_PLATFORM_NOT_IMPLEMENTED_MESSAGE =
      "Cloud platform not implemented";
  private final SnapshotRequestDao snapshotRequestDao;
  private final SnapshotBuilderSettingsDao snapshotBuilderSettingsDao;
  private final DatasetService datasetService;
  private final BigQueryDatasetPdao bigQueryDatasetPdao;

  private final AzureSynapsePdao azureSynapsePdao;
  private final QueryBuilderFactory queryBuilderFactory;

  public SnapshotBuilderService(
      SnapshotRequestDao snapshotRequestDao,
      SnapshotBuilderSettingsDao snapshotBuilderSettingsDao,
      DatasetService datasetService,
      BigQueryDatasetPdao bigQueryDatasetPdao,
      AzureSynapsePdao azureSynapsePdao,
      QueryBuilderFactory queryBuilderFactory) {
    this.snapshotRequestDao = snapshotRequestDao;
    this.snapshotBuilderSettingsDao = snapshotBuilderSettingsDao;
    this.datasetService = datasetService;
    this.bigQueryDatasetPdao = bigQueryDatasetPdao;
    this.azureSynapsePdao = azureSynapsePdao;
    this.queryBuilderFactory = queryBuilderFactory;
  }

  public SnapshotAccessRequestResponse createSnapshotRequest(
      UUID id, SnapshotAccessRequest snapshotAccessRequest, String email) {
    return snapshotRequestDao.create(id, snapshotAccessRequest, email);
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
      throw new NotImplementedException(CLOUD_PLATFORM_NOT_IMPLEMENTED_MESSAGE);
    }
  }

  public SnapshotBuilderGetConceptsResponse getConceptChildren(
      UUID datasetId, Integer conceptId, AuthenticatedUserRequest userRequest) {
    Dataset dataset = datasetService.retrieve(datasetId);
    TableNameGenerator tableNameGenerator = getTableNameGenerator(dataset, userRequest);
    String cloudSpecificSql =
        ConceptChildrenQueryBuilder.buildConceptChildrenQuery(
            conceptId, tableNameGenerator, CloudPlatformWrapper.of(dataset.getCloudPlatform()));
    List<SnapshotBuilderConcept> concepts =
        runSnapshotBuilderQuery(
            cloudSpecificSql,
            dataset,
            AggregateBQQueryResultsUtils::aggregateConceptResults,
            AggregateSynapseQueryResultsUtils::aggregateConceptResult);
    return new SnapshotBuilderGetConceptsResponse().result(concepts);
  }

  TableNameGenerator getTableNameGenerator(Dataset dataset, AuthenticatedUserRequest userRequest) {
    CloudPlatformWrapper platform = CloudPlatformWrapper.of(dataset.getCloudPlatform());
    if (platform.isGcp()) {
      return BigQueryVisitor.bqTableName(datasetService.retrieveModel(dataset, userRequest));
    } else if (platform.isAzure()) {
      return SynapseVisitor.azureTableName(
          datasetService.getOrCreateExternalAzureDataSource(dataset, userRequest));
    } else {
      throw new NotImplementedException(CLOUD_PLATFORM_NOT_IMPLEMENTED_MESSAGE);
    }
  }

  public SnapshotBuilderCountResponse getCountResponse(
      UUID id, List<SnapshotBuilderCohort> cohorts, AuthenticatedUserRequest userRequest) {
    int rollupCount =
        getRollupCountForCriteriaGroups(
            id,
            cohorts.stream().map(SnapshotBuilderCohort::getCriteriaGroups).toList(),
            userRequest);
    return new SnapshotBuilderCountResponse()
        .result(new SnapshotBuilderCountResponseResult().total(fuzzyLowCount(rollupCount)));
  }

  // if the rollup count is 0 OR >=20, then we will return the actual value
  // if the rollup count is between 1 and 19, we will return 19 and in the UI we will display <20
  // This helps reduce the risk of re-identification
  int fuzzyLowCount(int rollupCount) {
    return rollupCount == 0 ? rollupCount : Math.max(rollupCount, 19);
  }

  public EnumerateSnapshotAccessRequest enumerateByDatasetId(UUID id) {
    return convertToEnumerateModel(snapshotRequestDao.enumerateByDatasetId(id));
  }

  public SnapshotBuilderGetConceptsResponse searchConcepts(
      UUID datasetId, String domainId, String searchText, AuthenticatedUserRequest userRequest) {
    Dataset dataset = datasetService.retrieve(datasetId);
    TableNameGenerator tableNameGenerator = getTableNameGenerator(dataset, userRequest);
    SnapshotBuilderSettings snapshotBuilderSettings =
        snapshotBuilderSettingsDao.getSnapshotBuilderSettingsByDatasetId(datasetId);

    SnapshotBuilderDomainOption snapshotBuilderDomainOption =
        snapshotBuilderSettings.getDomainOptions().stream()
            .filter(domainOption -> domainOption.getName().equals(domainId))
            .findFirst()
            .orElseThrow(
                () ->
                    new BadRequestException(
                        "Invalid domain category is given: %s".formatted(domainId)));

    String cloudSpecificSql =
        SearchConceptsQueryBuilder.buildSearchConceptsQuery(
            snapshotBuilderDomainOption,
            searchText,
            tableNameGenerator,
            CloudPlatformWrapper.of(dataset.getCloudPlatform()));
    List<SnapshotBuilderConcept> concepts =
        runSnapshotBuilderQuery(
            cloudSpecificSql,
            dataset,
            AggregateBQQueryResultsUtils::aggregateConceptResults,
            AggregateSynapseQueryResultsUtils::aggregateConceptResult);
    return new SnapshotBuilderGetConceptsResponse().result(concepts);
  }

  public int getRollupCountForCriteriaGroups(
      UUID datasetId,
      List<List<SnapshotBuilderCriteriaGroup>> criteriaGroups,
      AuthenticatedUserRequest userRequest) {
    Dataset dataset = datasetService.retrieve(datasetId);
    SnapshotBuilderSettings snapshotBuilderSettings =
        snapshotBuilderSettingsDao.getSnapshotBuilderSettingsByDatasetId(datasetId);
    TableNameGenerator tableNameGenerator = getTableNameGenerator(dataset, userRequest);

    Query query =
        queryBuilderFactory
            .criteriaQueryBuilder("person", tableNameGenerator, snapshotBuilderSettings)
            .generateRollupCountsQueryForCriteriaGroupsList(criteriaGroups);
    String cloudSpecificSQL = query.renderSQL(CloudPlatformWrapper.of(dataset.getCloudPlatform()));

    return runSnapshotBuilderQuery(
            cloudSpecificSQL,
            dataset,
            AggregateBQQueryResultsUtils::rollupCountsMapper,
            AggregateSynapseQueryResultsUtils::rollupCountsMapper)
        .get(0);
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

  record ParentQueryResult(int parentId, int childId, String childName) {
    static List<ParentQueryResult> aggregateBigQuery(TableResult result) {
      return StreamSupport.stream(result.iterateAll().spliterator(), false)
          .map(
              row ->
                  new ParentQueryResult(
                      (int) row.get(HierarchyQueryBuilder.PARENT_ID).getLongValue(),
                      (int) row.get(HierarchyQueryBuilder.CONCEPT_ID).getLongValue(),
                      row.get(HierarchyQueryBuilder.CONCEPT_NAME).getStringValue()))
          .toList();
    }

    static ParentQueryResult aggregateAzure(ResultSet rs) {
      try {
        return new ParentQueryResult(
            (int) rs.getLong(HierarchyQueryBuilder.PARENT_ID),
            (int) rs.getLong(HierarchyQueryBuilder.CONCEPT_ID),
            rs.getString(HierarchyQueryBuilder.CONCEPT_NAME));
      } catch (SQLException e) {
        throw new ProcessResultSetException(
            "Error processing result set into SnapshotBuilderConcept model", e);
      }
    }
  }

  public SnapshotBuilderGetConceptHierarchyResponse getConceptHierarchy(
      UUID datasetId, int conceptId, AuthenticatedUserRequest userRequest) {
    Dataset dataset = datasetService.retrieve(datasetId);
    var query =
        queryBuilderFactory
            .hierarchyQueryBuilder(
                getTableNameGenerator(dataset, userRequest),
                snapshotBuilderSettingsDao.getSnapshotBuilderSettingsByDatasetId(datasetId))
            .generateQuery(conceptId);
    var sql = query.renderSQL(CloudPlatformWrapper.of(dataset.getCloudPlatform()));

    Map<Integer, SnapshotBuilderParentConcept> parents = new HashMap<>();
    runSnapshotBuilderQuery(
            sql, dataset, ParentQueryResult::aggregateBigQuery, ParentQueryResult::aggregateAzure)
        .forEach(
            row -> {
              SnapshotBuilderParentConcept parent =
                  parents.computeIfAbsent(
                      row.parentId,
                      k ->
                          new SnapshotBuilderParentConcept()
                              .parentId(k)
                              .children(new ArrayList<>()));
              parent.addChildrenItem(
                  new SnapshotBuilderConcept()
                      .id(row.childId)
                      .name(row.childName)
                      .hasChildren(true)
                      .count(1));
            });

    // Collect all children IDs
    var childrenIds =
        parents.values().stream()
            .flatMap(p -> p.getChildren().stream().map(SnapshotBuilderConcept::getId))
            .collect(Collectors.toSet());
    // The root is the one ID that is not the child of any other parent.
    var rootId =
        parents.keySet().stream().filter(k -> !childrenIds.contains(k)).findFirst().orElseThrow();
    var result = new ArrayList<SnapshotBuilderParentConcept>();
    result.add(parents.get(rootId));
    parents.entrySet().stream()
        .filter(entry -> !entry.getKey().equals(rootId))
        .map(Map.Entry::getValue)
        .forEach(result::add);

    return new SnapshotBuilderGetConceptHierarchyResponse().result(result);
  }
}
