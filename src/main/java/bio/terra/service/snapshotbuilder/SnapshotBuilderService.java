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
import bio.terra.service.snapshotbuilder.query.Query;
import bio.terra.service.snapshotbuilder.query.SqlRenderContext;
import bio.terra.service.snapshotbuilder.query.TableNameGenerator;
import bio.terra.service.snapshotbuilder.utils.AggregateBQQueryResultsUtils;
import bio.terra.service.snapshotbuilder.utils.AggregateSynapseQueryResultsUtils;
import bio.terra.service.snapshotbuilder.utils.QueryBuilderFactory;
import bio.terra.service.snapshotbuilder.utils.constants.Concept;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDatasetPdao;
import com.google.cloud.bigquery.FieldValueList;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class SnapshotBuilderService {

  private static final Logger logger = LoggerFactory.getLogger(SnapshotBuilderService.class);

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
      Query query,
      Dataset dataset,
      AuthenticatedUserRequest userRequest,
      BigQueryDatasetPdao.Converter<T> bqConverter,
      AzureSynapsePdao.Converter<T> synapseConverter) {
    String sql = query.renderSQL(createContext(dataset, userRequest));
    Instant start = Instant.now();
    List<T> result =
        CloudPlatformWrapper.of(dataset.getCloudPlatform())
            .choose(
                () -> bigQueryDatasetPdao.runQuery(sql, dataset, bqConverter),
                () -> azureSynapsePdao.runQuery(sql, synapseConverter));
    logger.info(
        "{} seconds to run query \"{}\"", Duration.between(start, Instant.now()).toSeconds(), sql);
    return result;
  }

  public SnapshotBuilderGetConceptsResponse getConceptChildren(
      UUID datasetId, int conceptId, AuthenticatedUserRequest userRequest) {
    Dataset dataset = datasetService.retrieve(datasetId);

    SnapshotBuilderDomainOption domainOption = getDomainOption(conceptId, dataset, userRequest);

    Query query =
        queryBuilderFactory
            .conceptChildrenQueryBuilder()
            .buildConceptChildrenQuery(domainOption, conceptId);

    List<SnapshotBuilderConcept> concepts =
        runSnapshotBuilderQuery(
            query,
            dataset,
            userRequest,
            AggregateBQQueryResultsUtils::toConcept,
            AggregateSynapseQueryResultsUtils::toConcept);
    return new SnapshotBuilderGetConceptsResponse().result(concepts);
  }

  public SqlRenderContext createContext(Dataset dataset, AuthenticatedUserRequest userRequest) {
    CloudPlatformWrapper platform = CloudPlatformWrapper.of(dataset.getCloudPlatform());
    TableNameGenerator tableNameGenerator =
        CloudPlatformWrapper.of(dataset.getCloudPlatform())
            .choose(
                () ->
                    BigQueryVisitor.bqTableName(datasetService.retrieveModel(dataset, userRequest)),
                () ->
                    SynapseVisitor.azureTableName(
                        datasetService.getOrCreateExternalAzureDataSource(dataset, userRequest)));
    return new SqlRenderContext(tableNameGenerator, platform);
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

    Query query =
        queryBuilderFactory
            .searchConceptsQueryBuilder()
            .buildSearchConceptsQuery(snapshotBuilderDomainOption, searchText);

    List<SnapshotBuilderConcept> concepts =
        runSnapshotBuilderQuery(
            query,
            dataset,
            userRequest,
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

    Query query =
        queryBuilderFactory
            .criteriaQueryBuilder("person", snapshotBuilderSettings)
            .generateRollupCountsQueryForCriteriaGroupsList(criteriaGroups);

    return runSnapshotBuilderQuery(
            query,
            dataset,
            userRequest,
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

  private String getDomainId(int conceptId, Dataset dataset, AuthenticatedUserRequest userRequest) {
    List<String> domainIdResult =
        runSnapshotBuilderQuery(
            queryBuilderFactory.conceptChildrenQueryBuilder().retrieveDomainId(conceptId),
            dataset,
            userRequest,
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

  record ParentQueryResult(
      int parentId, int childId, String childName, String code, int count, boolean hasChildren) {
    ParentQueryResult(ResultSet rs) throws SQLException {
      this(
          rs.getInt(QueryBuilderFactory.PARENT_ID),
          rs.getInt(Concept.CONCEPT_ID),
          rs.getString(Concept.CONCEPT_NAME),
          rs.getString(Concept.CONCEPT_CODE),
          rs.getInt(QueryBuilderFactory.COUNT),
          rs.getBoolean(QueryBuilderFactory.HAS_CHILDREN));
    }

    ParentQueryResult(FieldValueList row) {
      this(
          (int) row.get(QueryBuilderFactory.PARENT_ID).getLongValue(),
          (int) row.get(Concept.CONCEPT_ID).getLongValue(),
          row.get(Concept.CONCEPT_NAME).getStringValue(),
          row.get(Concept.CONCEPT_CODE).getStringValue(),
          (int) row.get(QueryBuilderFactory.COUNT).getLongValue(),
          row.get(QueryBuilderFactory.HAS_CHILDREN).getBooleanValue());
    }

    SnapshotBuilderConcept toConcept() {
      return new SnapshotBuilderConcept()
          .id(childId)
          .name(childName)
          .hasChildren(hasChildren)
          .code(code)
          .count(SnapshotBuilderService.fuzzyLowCount(count));
    }
  }

  public SnapshotBuilderGetConceptHierarchyResponse getConceptHierarchy(
      UUID datasetId, int conceptId, AuthenticatedUserRequest userRequest) {
    Dataset dataset = datasetService.retrieve(datasetId);

    var domainOption = getDomainOption(conceptId, dataset, userRequest);
    var query = queryBuilderFactory.hierarchyQueryBuilder().generateQuery(domainOption, conceptId);

    Map<Integer, SnapshotBuilderParentConcept> parents = new HashMap<>();
    runSnapshotBuilderQuery(
            query, dataset, userRequest, ParentQueryResult::new, ParentQueryResult::new)
        .forEach(
            row -> {
              SnapshotBuilderParentConcept parent =
                  parents.computeIfAbsent(
                      row.parentId,
                      k ->
                          new SnapshotBuilderParentConcept()
                              .parentId(k)
                              .children(new ArrayList<>()));
              parent.addChildrenItem(row.toConcept());
            });
    if (parents.isEmpty()) {
      throw new BadRequestException("No parents found for concept %s".formatted(conceptId));
    }

    return new SnapshotBuilderGetConceptHierarchyResponse().result(List.copyOf(parents.values()));
  }

  private SnapshotBuilderDomainOption getDomainOption(
      int conceptId, Dataset dataset, AuthenticatedUserRequest userRequest) {
    // domain is needed to join with the domain specific occurrence table
    // this does not work for the metadata domain
    String domainId = getDomainId(conceptId, dataset, userRequest);
    return getDomainOptionFromSettingsByName(domainId, dataset.getId());
  }
}
