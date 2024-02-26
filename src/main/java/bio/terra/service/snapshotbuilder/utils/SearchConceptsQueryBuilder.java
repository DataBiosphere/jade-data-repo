package bio.terra.service.snapshotbuilder.utils;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.service.snapshotbuilder.query.FieldPointer;
import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.FilterVariable;
import bio.terra.service.snapshotbuilder.query.Literal;
import bio.terra.service.snapshotbuilder.query.Query;
import bio.terra.service.snapshotbuilder.query.TableNameGenerator;
import bio.terra.service.snapshotbuilder.query.TablePointer;
import bio.terra.service.snapshotbuilder.query.TableVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BinaryFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BooleanAndOrFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.FunctionFilterVariable;
import java.util.List;

public class SearchConceptsQueryBuilder {

  private SearchConceptsQueryBuilder() {}

  public static String buildSearchConceptsQuery(
      String domainId,
      String searchText,
      TableNameGenerator tableNameGenerator,
      CloudPlatformWrapper platform) {
    var conceptTablePointer = TablePointer.fromTableName("concept", tableNameGenerator);
    var conditionOccurrencePointer = TablePointer.fromTableName("condition_occurrence", tableNameGenerator);
    var conceptTableVariable = TableVariable.forPrimary(conceptTablePointer);
    var nameField = conceptTableVariable.makeFieldVariable("concept_name");
    var idField = conceptTableVariable.makeFieldVariable("concept_id");

    // FROM concept JOIN condition_occurrence ON condition_occurrence.concept_id = concept.concept_id
    var conditionOccurenceTableVariable =
        TableVariable.forJoined(conditionOccurrencePointer, "condition_concept_id", idField);

    var personIdField =
        new FieldVariable(
            new FieldPointer(conditionOccurrencePointer, "person_id", "COUNT"),
            conditionOccurenceTableVariable,
            null,
            true);

    // domain clause filters for the given domain id based on field domain_id
    var domainClause = createDomainClause(conceptTablePointer, conceptTableVariable, domainId);

    // if the search test is empty do not include the search clauses
    // return all concepts in the specified domain
    if (searchText == null || searchText.isEmpty()) {
      // TODO: DC-845 Implement pagination, remove hardcoded limit
      Query query =
          new Query(List.of(nameField, idField), List.of(conceptTableVariable), domainClause, 100);
      return query.renderSQL(platform);
    }

    // search concept name clause filters for the search text based on field concept_name
    var searchNameClause =
        createSearchConceptClause(
            conceptTablePointer, conceptTableVariable, searchText, "concept_name");

    // search concept name clause filters for the search text based on field concept_code
    var searchCodeClause =
        createSearchConceptClause(
            conceptTablePointer, conceptTableVariable, searchText, "concept_code");

    // SearchConceptNameClause OR searchCodeClause
    List<FilterVariable> searches = List.of(searchNameClause, searchCodeClause);
    BooleanAndOrFilterVariable searchClause =
        new BooleanAndOrFilterVariable(BooleanAndOrFilterVariable.LogicalOperator.OR, searches);

    // domainClause AND (searchNameClause OR searchCodeClause)
    List<FilterVariable> allFilters = List.of(domainClause, searchClause);
    BooleanAndOrFilterVariable whereClause =
        new BooleanAndOrFilterVariable(BooleanAndOrFilterVariable.LogicalOperator.AND, allFilters);

    // SELECT concept_name, concept_id, COUNT(DISTINCT person_id)
    // FROM concept JOIN condition_occurrence ON condition_occurrence.concept_id =  concept.concept_id
    // WHERE concept.name CONTAINS {{name}} GROUP BY condition_occurrence.concept_id

    Query query =
        new Query(
            List.of(nameField, idField, personIdField),
            List.of(conceptTableVariable, conditionOccurenceTableVariable),
            whereClause,
            100);

    return query.renderSQL(platform);
  }

  static FunctionFilterVariable createSearchConceptClause(
      TablePointer conceptTablePointer,
      TableVariable conceptTableVariable,
      String searchText,
      String columnName) {
    return new FunctionFilterVariable(
        FunctionFilterVariable.FunctionTemplate.TEXT_EXACT_MATCH,
        new FieldVariable(new FieldPointer(conceptTablePointer, columnName), conceptTableVariable),
        new Literal(searchText));
  }

  static BinaryFilterVariable createDomainClause(
      TablePointer conceptTablePointer, TableVariable conceptTableVariable, String domainId) {
    return new BinaryFilterVariable(
        new FieldVariable(new FieldPointer(conceptTablePointer, "domain_id"), conceptTableVariable),
        BinaryFilterVariable.BinaryOperator.EQUALS,
        new Literal(domainId));
  }
}
