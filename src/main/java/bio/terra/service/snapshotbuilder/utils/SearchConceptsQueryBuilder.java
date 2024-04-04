package bio.terra.service.snapshotbuilder.utils;

import bio.terra.model.SnapshotBuilderDomainOption;
import bio.terra.service.snapshotbuilder.query.FieldPointer;
import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.FilterVariable;
import bio.terra.service.snapshotbuilder.query.Literal;
import bio.terra.service.snapshotbuilder.query.OrderByDirection;
import bio.terra.service.snapshotbuilder.query.OrderByVariable;
import bio.terra.service.snapshotbuilder.query.Query;
import bio.terra.service.snapshotbuilder.query.SelectExpression;
import bio.terra.service.snapshotbuilder.query.TableNameGenerator;
import bio.terra.service.snapshotbuilder.query.TablePointer;
import bio.terra.service.snapshotbuilder.query.TableVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BinaryFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BooleanAndOrFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.FunctionFilterVariable;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

public class SearchConceptsQueryBuilder {

  public static final String CONCEPT_ID = "concept_id";
  public static final String CONCEPT = "concept";
  public static final String CONCEPT_ANCESTOR = "concept_ancestor";
  public static final String CONCEPT_NAME = "concept_name";
  public static final String CONCEPT_CODE = "concept_code";
  public final String ANCESTOR_CONCEPT_ID = "ancestor_concept_id";
  public final String DESCENDANT_CONCEPT_ID = "descendant_concept_id";
  public final TableNameGenerator tableNameGenerator;

  SearchConceptsQueryBuilder(TableNameGenerator tableNameGenerator) {
    this.tableNameGenerator = tableNameGenerator;
  }
  /**
   * Generate a query that retrieves all the concepts from the given searched text. If a search text
   * is not provided, a search will be made on the domain.
   */
  public Query buildSearchConceptsQuery(
      SnapshotBuilderDomainOption domainOption, String searchText) {
    var conceptTablePointer = TablePointer.fromTableName(CONCEPT, tableNameGenerator);
    var conceptAncestorPointer = TablePointer.fromTableName(CONCEPT_ANCESTOR, tableNameGenerator);
    var domainOccurrencePointer =
        TablePointer.fromTableName(domainOption.getTableName(), tableNameGenerator);
    var conceptTableVariable = TableVariable.forPrimary(conceptTablePointer);
    var nameField = conceptTableVariable.makeFieldVariable(CONCEPT_NAME);
    var idField = conceptTableVariable.makeFieldVariable(CONCEPT_ID);

    // FROM concept JOIN concept_ancestor ON ancestor_concept_id = concept.concept_id
    var conceptAncestorTableVariable =
        TableVariable.forJoined(conceptAncestorPointer, ANCESTOR_CONCEPT_ID, idField);
    var descendantIdFieldVariable =
        conceptAncestorTableVariable.makeFieldVariable(DESCENDANT_CONCEPT_ID);

    // LEFT JOIN domainOccurrencePointer ON domainOccurrencePointer.concept_id =
    // concept_ancestor.descendant_concept_id
    var domainOccurenceTableVariable =
        TableVariable.forLeftJoined(
            domainOccurrencePointer, domainOption.getColumnName(), descendantIdFieldVariable);

    var countField =
        new FieldVariable(
            new FieldPointer(
                domainOccurrencePointer, CriteriaQueryBuilder.PERSON_ID_FIELD_NAME, "COUNT"),
            domainOccurenceTableVariable,
            "count",
            true);

    // domain clause filters for the given domain id based on field domain_id
    var domainClause =
        createDomainClause(conceptTablePointer, conceptTableVariable, domainOption.getName());

    List<SelectExpression> select = List.of(nameField, idField, countField);

    List<TableVariable> tables =
        List.of(conceptTableVariable, conceptAncestorTableVariable, domainOccurenceTableVariable);

    List<OrderByVariable> orderBy =
        List.of(new OrderByVariable(countField, OrderByDirection.DESCENDING));

    List<FieldVariable> groupBy = List.of(nameField, idField);

    FilterVariable where;
    if (StringUtils.isEmpty(searchText)) {
      where = domainClause;
    } else {
      // search concept name clause filters for the search text based on field concept_name
      var searchNameClause =
          createSearchConceptClause(
              conceptTablePointer, conceptTableVariable, searchText, CONCEPT_NAME);

      // search concept name clause filters for the search text based on field concept_code
      var searchCodeClause =
          createSearchConceptClause(
              conceptTablePointer, conceptTableVariable, searchText, CONCEPT_CODE);

      // (searchNameClause OR searchCodeClause)
      List<FilterVariable> searches = List.of(searchNameClause, searchCodeClause);
      BooleanAndOrFilterVariable searchClause =
          new BooleanAndOrFilterVariable(BooleanAndOrFilterVariable.LogicalOperator.OR, searches);

      // domainClause AND (searchNameClause OR searchCodeClause)
      List<FilterVariable> allFilters = List.of(domainClause, searchClause);

      where =
          new BooleanAndOrFilterVariable(
              BooleanAndOrFilterVariable.LogicalOperator.AND, allFilters);
    }

    // SELECT concept_name, concept_id, COUNT(DISTINCT person_id) as count
    // FROM concept JOIN domain_occurrence ON domain_occurrence.concept_id =
    // concept.concept_id
    // WHERE concept.name CONTAINS {{name}} GROUP BY c.name, c.concept_id
    // ORDER BY count DESC

    return new Query(select, tables, where, groupBy, orderBy, 100);
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
