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
import bio.terra.service.snapshotbuilder.query.TableNameGenerator;
import bio.terra.service.snapshotbuilder.utils.AggregateBQQueryResultsUtils;
import bio.terra.service.snapshotbuilder.utils.AggregateSynapseQueryResultsUtils;
import bio.terra.service.snapshotbuilder.utils.HierarchyQueryBuilder;
import bio.terra.service.snapshotbuilder.utils.QueryBuilderFactory;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDatasetPdao;
import com.google.cloud.bigquery.FieldValueList;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
      String sql,
      Dataset dataset,
      BigQueryDatasetPdao.Converter<T> bqConverter,
      AzureSynapsePdao.Converter<T> synapseConverter) {
    return CloudPlatformWrapper.of(dataset.getCloudPlatform())
        .choose(
            () -> bigQueryDatasetPdao.runQuery(sql, dataset, bqConverter),
            () -> azureSynapsePdao.runQuery(sql, synapseConverter));
  }

  public SnapshotBuilderGetConceptsResponse getConceptChildren(
      UUID datasetId, Integer conceptId, AuthenticatedUserRequest userRequest) {
    Dataset dataset = datasetService.retrieve(datasetId);
    CloudPlatformWrapper cloudPlatform = CloudPlatformWrapper.of(dataset.getCloudPlatform());
    TableNameGenerator tableNameGenerator = getTableNameGenerator(dataset, userRequest);

    // domain is needed to join with the domain specific occurrence table
    // this does not work for the metadata domain
    String domainId = getDomainId(conceptId, tableNameGenerator, dataset);
    SnapshotBuilderDomainOption domainOption =
        getDomainOptionFromSettingsByName(domainId, datasetId);

    String cloudSpecificSql =
        queryBuilderFactory
            .conceptChildrenQueryBuilder(tableNameGenerator)
            .buildConceptChildrenQuery(domainOption, conceptId)
            .renderSQL(cloudPlatform);

    List<SnapshotBuilderConcept> concepts =
        runSnapshotBuilderQuery(
            cloudSpecificSql,
            dataset,
            AggregateBQQueryResultsUtils::toConcept,
            AggregateSynapseQueryResultsUtils::toConcept);
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
  public static int fuzzyLowCount(int rollupCount) {
    return rollupCount == 0 ? rollupCount : Math.max(rollupCount, 19);
  }

  public EnumerateSnapshotAccessRequest enumerateByDatasetId(UUID id) {
    return convertToEnumerateModel(snapshotRequestDao.enumerateByDatasetId(id));
  }

  public SnapshotBuilderGetConceptsResponse searchConcepts(
      UUID datasetId, int domainId, String searchText, AuthenticatedUserRequest userRequest) {
    Dataset dataset = datasetService.retrieve(datasetId);
    TableNameGenerator tableNameGenerator = getTableNameGenerator(dataset, userRequest);
    SnapshotBuilderSettings snapshotBuilderSettings =
        snapshotBuilderSettingsDao.getSnapshotBuilderSettingsByDatasetId(datasetId);

    SnapshotBuilderDomainOption snapshotBuilderDomainOption =
        snapshotBuilderSettings.getDomainOptions().stream()
            .filter(domainOption -> domainOption.getId() == domainId)
            .findFirst()
            .orElseThrow(
                () ->
                    new BadRequestException(
                        "Invalid domain category is given: %s".formatted(domainId)));

    String cloudSpecificSql =
        queryBuilderFactory
            .searchConceptsQueryBuilder(tableNameGenerator)
            .buildSearchConceptsQuery(snapshotBuilderDomainOption, searchText)
            .renderSQL(CloudPlatformWrapper.of(dataset.getCloudPlatform()));

    List<SnapshotBuilderConcept> concepts =
        runSnapshotBuilderQuery(
            cloudSpecificSql,
            dataset,
            AggregateBQQueryResultsUtils::toConcept,
            AggregateSynapseQueryResultsUtils::toConcept);
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

    String cloudSpecificSQL =
        queryBuilderFactory
            .criteriaQueryBuilder("person", tableNameGenerator, snapshotBuilderSettings)
            .generateRollupCountsQueryForCriteriaGroupsList(criteriaGroups)
            .renderSQL(CloudPlatformWrapper.of(dataset.getCloudPlatform()));

    return runSnapshotBuilderQuery(
            cloudSpecificSQL,
            dataset,
            AggregateBQQueryResultsUtils::toCount,
            AggregateSynapseQueryResultsUtils::toCount)
        .get(0);
  }

  private SnapshotBuilderDomainOption getDomainOptionFromSettingsByName(
      String domainId, UUID datasetId) {
    SnapshotBuilderSettings snapshotBuilderSettings =
        snapshotBuilderSettingsDao.getSnapshotBuilderSettingsByDatasetId(datasetId);

    return snapshotBuilderSettings.getDomainOptions().stream()
        .filter(domainOption -> domainOption.getName().equals(domainId))
        .findFirst()
        .orElseThrow(
            () ->
                new BadRequestException(
                    "Invalid domain category is given: %s".formatted(domainId)));
  }

  private String getDomainId(
      Integer conceptId, TableNameGenerator tableNameGenerator, Dataset dataset) {
    CloudPlatformWrapper cloudPlatform = CloudPlatformWrapper.of(dataset.getCloudPlatform());
    List<String> domainIdResult =
        runSnapshotBuilderQuery(
            queryBuilderFactory
                .conceptChildrenQueryBuilder(tableNameGenerator)
                .retrieveDomainId(conceptId)
                .renderSQL(cloudPlatform),
            dataset,
            AggregateBQQueryResultsUtils::toDomainId,
            AggregateSynapseQueryResultsUtils::toDomainId);
    if (domainIdResult.size() == 1) {
      return domainIdResult.get(0);
    } else if (domainIdResult.isEmpty()) {
      throw new IllegalStateException("No domain Id found for concept: " + conceptId);
    } else {
      throw new IllegalStateException("Multiple domains found for concept: " + conceptId);
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

  record ParentQueryResult(int parentId, int childId, String childName) {
    ParentQueryResult(ResultSet rs) throws SQLException {
      this(
          rs.getInt(HierarchyQueryBuilder.PARENT_ID),
          rs.getInt(HierarchyQueryBuilder.CONCEPT_ID),
          rs.getString(HierarchyQueryBuilder.CONCEPT_NAME));
    }

    ParentQueryResult(FieldValueList row) {
      this(
          (int) row.get(HierarchyQueryBuilder.PARENT_ID).getLongValue(),
          (int) row.get(HierarchyQueryBuilder.CONCEPT_ID).getLongValue(),
          row.get(HierarchyQueryBuilder.CONCEPT_NAME).getStringValue());
    }
  }

  public SnapshotBuilderGetConceptHierarchyResponse getConceptHierarchy(
      UUID datasetId, int conceptId, AuthenticatedUserRequest userRequest) {
    Dataset dataset = datasetService.retrieve(datasetId);
    String cloudSpecificSql =
        queryBuilderFactory
            .hierarchyQueryBuilder(getTableNameGenerator(dataset, userRequest))
            .generateQuery(conceptId)
            .renderSQL(CloudPlatformWrapper.of(dataset.getCloudPlatform()));

    Map<Integer, SnapshotBuilderParentConcept> parents = new HashMap<>();
    runSnapshotBuilderQuery(
            cloudSpecificSql, dataset, ParentQueryResult::new, ParentQueryResult::new)
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
    if (parents.isEmpty()) {
      throw new BadRequestException("No parents found for concept %s".formatted(conceptId));
    }

    return new SnapshotBuilderGetConceptHierarchyResponse().result(List.copyOf(parents.values()));
  }
}
