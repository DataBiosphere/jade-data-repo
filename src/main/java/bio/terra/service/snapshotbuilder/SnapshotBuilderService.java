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
import bio.terra.model.SnapshotBuilderSettings;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.snapshotbuilder.query.Query;
import bio.terra.service.snapshotbuilder.query.TableNameGenerator;
import bio.terra.service.snapshotbuilder.utils.AggregateBQQueryResultsUtils;
import bio.terra.service.snapshotbuilder.utils.AggregateSynapseQueryResultsUtils;
import bio.terra.service.snapshotbuilder.utils.ConceptChildrenQueryBuilder;
import bio.terra.service.snapshotbuilder.utils.CriteriaQueryBuilderFactory;
import bio.terra.service.snapshotbuilder.utils.SearchConceptsQueryBuilder;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDatasetPdao;
import com.google.cloud.bigquery.TableResult;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
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
  private final CriteriaQueryBuilderFactory criteriaQueryBuilderFactory;

  public SnapshotBuilderService(
      SnapshotRequestDao snapshotRequestDao,
      SnapshotBuilderSettingsDao snapshotBuilderSettingsDao,
      DatasetService datasetService,
      BigQueryDatasetPdao bigQueryDatasetPdao,
      AzureSynapsePdao azureSynapsePdao,
      CriteriaQueryBuilderFactory criteriaQueryBuilderFactory) {
    this.snapshotRequestDao = snapshotRequestDao;
    this.snapshotBuilderSettingsDao = snapshotBuilderSettingsDao;
    this.datasetService = datasetService;
    this.bigQueryDatasetPdao = bigQueryDatasetPdao;
    this.azureSynapsePdao = azureSynapsePdao;
    this.criteriaQueryBuilderFactory = criteriaQueryBuilderFactory;
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
  public static int fuzzyLowCount(int rollupCount) {
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
        criteriaQueryBuilderFactory
            .createCriteriaQueryBuilder("person", tableNameGenerator, snapshotBuilderSettings)
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

  // Hardcoded stubbed data, will remove once query is implemented.
  private static SnapshotBuilderConcept createConcept(int id, String name, boolean hasChildren) {
    return new SnapshotBuilderConcept().id(id).name(name).count(100).hasChildren(hasChildren);
  }

  private static List<SnapshotBuilderConcept> loadConcepts() {
    return List.of(
        createConcept(100, "Condition", true),
        createConcept(400, "Carcinoma of lung parenchyma", true),
        createConcept(401, "Squamous cell carcinoma of lung", true),
        createConcept(402, "Non-small cell lung cancer", true),
        createConcept(403, "Epidermal growth factor receptor negative ...", false),
        createConcept(404, "Non-small cell lung cancer with mutation in epidermal..", true),
        createConcept(405, "Non-small cell cancer of lung biopsy..", false),
        createConcept(406, "Non-small cell cancer of lung lymph node..", false),
        createConcept(407, "Small cell lung cancer", true),
        createConcept(408, "Lung Parenchcyma", false));
  }

  private static SnapshotBuilderConcept getConcept(List<SnapshotBuilderConcept> concepts, int id) {
    return concepts.stream().filter(c -> c.getId().equals(id)).findFirst().orElseThrow();
  }

  // Map of concept id to parent concept id.
  private static final Map<Integer, Integer> HIERARCHY =
      Map.of(406, 404, 405, 404, 404, 401, 403, 401, 407, 402, 401, 400, 400, 100, 408, 100);

  public SnapshotBuilderGetConceptHierarchyResponse getConceptHierarchy(
      UUID id, int conceptId, AuthenticatedUserRequest userRequest) {
    var concepts = loadConcepts();
    SnapshotBuilderConcept concept = getConcept(concepts, conceptId);
    while (true) {
      // For a child concept, generate its parent concept and siblings until we reach the root.
      var parentId = HIERARCHY.get(concept.getId());
      if (parentId == null) {
        break;
      }
      var parent = getConcept(concepts, parentId);
      concept =
          parent.children(
              HIERARCHY.entrySet().stream()
                  .filter(e -> e.getValue().equals(parentId))
                  .map(entry -> getConcept(concepts, entry.getKey()))
                  .toList());
    }
    return new SnapshotBuilderGetConceptHierarchyResponse().result(concept);
  }
}
