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
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.snapshotbuilder.query.FieldPointer;
import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.Literal;
import bio.terra.service.snapshotbuilder.query.Query;
import bio.terra.service.snapshotbuilder.query.TablePointer;
import bio.terra.service.snapshotbuilder.query.TableVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BinaryFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.SubQueryFilterVariable;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;
import java.util.ArrayList;
import java.util.Collections;
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
  private final DatasetService datasetService;

  public SnapshotBuilderService(
      SnapshotRequestDao snapshotRequestDao, DatasetService datasetService) {
    this.snapshotRequestDao = snapshotRequestDao;
    this.datasetService = datasetService;
  }

  public SnapshotAccessRequestResponse createSnapshotRequest(
      UUID id, SnapshotAccessRequest snapshotAccessRequest, String email) {
    return snapshotRequestDao.create(id, snapshotAccessRequest, email);
  }

  public SnapshotBuilderGetConceptsResponse getConceptChildren(
      UUID datasetId, Integer conceptId, AuthenticatedUserRequest userRequest) {
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
            new Literal(conceptId));
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
    Map<String, DatasetModel> datasetMap = Collections.singletonMap(dataset.getName(), dataset);
    BigQueryVisitor bqVisitor = new BigQueryVisitor(datasetMap);
    String bqSQL = grammarQuery.translateSql(bqVisitor);
    logger.info(bqSQL);

    // Execute query
    //    RowMapper<SnapshotBuilderConcept> rowMapper =
    //        (rs, rowNum) ->
    //            new SnapshotBuilderConcept()
    //                    .id(rs.getInt("concept_id"))
    //                    .name(rs.getString("name"))
    //                    .hasChildren(true)
    //                    .count(100);

    TableResult tableResult;
    List<SnapshotBuilderConcept> conceptList = new ArrayList<>();
    try {
      tableResult = datasetService.query(bqSQL, datasetId);
      for (FieldValueList fieldValues : tableResult.iterateAll()) {
        conceptList.add(mapToConcept(fieldValues));
      }
    } catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }

    return new SnapshotBuilderGetConceptsResponse().result(conceptList);
  }

  private SnapshotBuilderConcept mapToConcept(FieldValueList value) {
    return new SnapshotBuilderConcept()
        .id(Integer.valueOf(value.get("concept_id").getStringValue()))
        .name(value.get("name").getStringValue())
        .hasChildren(true)
        .count(100);
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
