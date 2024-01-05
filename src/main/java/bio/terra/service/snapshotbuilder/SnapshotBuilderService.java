package bio.terra.service.snapshotbuilder;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.grammar.google.BigQueryVisitor;
import bio.terra.model.DatasetModel;
import bio.terra.model.SnapshotAccessRequest;
import bio.terra.model.SnapshotAccessRequestResponse;
import bio.terra.model.SnapshotBuilderCohort;
import bio.terra.model.SnapshotBuilderConcept;
import bio.terra.model.SnapshotBuilderCountResponse;
import bio.terra.model.SnapshotBuilderCountResponseResult;
import bio.terra.model.SnapshotBuilderGetConceptsResponse;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.snapshotbuilder.query.FieldPointer;
import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.Literal;
import bio.terra.service.snapshotbuilder.query.Query;
import bio.terra.service.snapshotbuilder.query.TablePointer;
import bio.terra.service.snapshotbuilder.query.TableVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BinaryFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.SubQueryFilterVariable;
import bio.terra.service.tabulardata.google.bigquery.BigQueryPdao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class SnapshotBuilderService {
  private static final Logger logger = LoggerFactory.getLogger(SnapshotBuilderService.class);

  private final SnapshotRequestDao snapshotRequestDao;
  private final DatasetService datasetService;
  private final BigQueryPdao bigQueryPdao;

  public SnapshotBuilderService(SnapshotRequestDao snapshotRequestDao, DatasetService datasetService, BigQueryPdao bigQueryPdao) {
    this.snapshotRequestDao = snapshotRequestDao;
    this.datasetService = datasetService;
    this.bigQueryPdao = bigQueryPdao;
  }

  public SnapshotAccessRequestResponse createSnapshotRequest(
      UUID id, SnapshotAccessRequest snapshotAccessRequest, String email) {
    return snapshotRequestDao.create(id, snapshotAccessRequest, email);
  }

  public SnapshotBuilderGetConceptsResponse getConceptChildren(UUID datasetId, Integer conceptId,
                                                               AuthenticatedUserRequest userRequest) {
    // TODO: Build real query - this should get the name and ID from the concept table, the count
    // from the occurrence table, and the existence of children from the concept_ancestor table.

    // Build the query
    TablePointer conceptTablePointer = TablePointer.fromTableName("concept");
    TableVariable conceptTableVariable = TableVariable.forPrimary(conceptTablePointer);
    FieldPointer nameFieldPointer = new FieldPointer(conceptTablePointer, "name");
    FieldVariable nameFieldVariable = new FieldVariable(nameFieldPointer, conceptTableVariable);
    FieldPointer idFieldPointer = new FieldPointer(conceptTablePointer, "concept_id");
    FieldVariable idFieldVariable = new FieldVariable(idFieldPointer, conceptTableVariable);

    TablePointer tablePointer = TablePointer.fromTableName("concept_ancestor");
    TableVariable tableVariable = TableVariable.forPrimary(tablePointer);
    FieldPointer fieldPointer = new FieldPointer(tablePointer, "descendant_concept_id");
    FieldVariable fieldVariable = new FieldVariable(fieldPointer, tableVariable);
    BinaryFilterVariable whereClause =
        new BinaryFilterVariable(
            new FieldVariable(new FieldPointer(tablePointer, "ancestor_concept_id"), tableVariable),
            BinaryFilterVariable.BinaryOperator.EQUALS,
            new Literal(316139));
    Query subQuery = new Query(List.of(fieldVariable), List.of(tableVariable), whereClause);
    SubQueryFilterVariable subQueryFilterVariable =
        new SubQueryFilterVariable(idFieldVariable, SubQueryFilterVariable.Operator.IN, subQuery);
    Query query =
        new Query(
            List.of(nameFieldVariable, idFieldVariable),
            List.of(conceptTableVariable),
            subQueryFilterVariable);
    String sql = query.renderSQL();

    // Translate query to BigQuery SQL
    bio.terra.grammar.Query grammarQuery = bio.terra.grammar.Query.parse(sql);
    DatasetModel dataset = datasetService.retrieveDatasetModel(datasetId, userRequest);
    Map<String, DatasetModel> datasetMap =
        Collections.singletonMap(dataset.getName(), dataset);
     BigQueryVisitor bqVisitor = new BigQueryVisitor(datasetMap);
    String bqSQL = grammarQuery.translateSql(bqVisitor);
    logger.info(bqSQL);

    // Execute query
    //bigQueryPdao


    return new SnapshotBuilderGetConceptsResponse()
        .result(
            List.of(
                new SnapshotBuilderConcept()
                    .count(100)
                    .name("Stub concept")
                    .hasChildren(true)
                    .id(conceptId + 1)));
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
}
