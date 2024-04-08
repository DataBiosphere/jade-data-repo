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
   * is not provided, a search will be made only on the domain.
   *
   * <Query code>
   *   GCP:
   *   SELECT c.concept_name, c_concept_id, COUNT(DISTINCT co.person_id) AS count FROM `concept` AS c
   *   JOIN `concept_ancestor` AS c0 ON c0.ancestor_concept_id = c.concept_id
   *   LEFT JOIN `'domain'_occurrence` AS co ON co.'domain'_concept_id = c0.descendant_concept_id
   *   WHERE (c.domain_id = 'domain'
   *   AND (CONTAINS_SUBSTR(c.concept_name, 'search_text')
   *   OR CONTAINS_SUBSTR(c.concept_code, 'search_text')))
   *   GROUP BY c.concept_name, c.concept_id
   *   ORDER BY count DESC
   *   LIMIT 100
   *
   *   AZURE:
   *   SELECT c.concept_name, c_concept_id, COUNT(DISTINCT co.person_id) AS count FROM `concept` AS c
   *   JOIN `concept_ancestor` AS c0 ON c0.ancestor_concept_id = c.concept_id
   *   LEFT JOIN `'domain'_occurrence` AS co ON co.'domain'_concept_id = c0.descendant_concept_id
   *   WHERE (c.domain_id = 'domain'
   *   AND (CHARINDEX('search_text', c.concept_name) > 0
   *   OR CHARINDEX('search_text', c.concept_code) > 0))
   *   GROUP BY c.concept_name, c.concept_id
   *   ORDER BY count DESC
   *   LIMIT 100
   * </Query code>
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

    // FROM 'concept' as c
    // JOIN concept_ancestor as c0 ON c0.ancestor_concept_id = c.concept_id
    var conceptAncestorTableVariable =
        TableVariable.forJoined(conceptAncestorPointer, ANCESTOR_CONCEPT_ID, idField);

    var descendantIdFieldVariable =
        conceptAncestorTableVariable.makeFieldVariable(DESCENDANT_CONCEPT_ID);

    // LEFT JOIN `'domain'_occurrence as co ON 'domain_occurrence'.concept_id =
    // concept_ancestor.descendant_concept_id
    var domainOccurenceTableVariable =
        TableVariable.forLeftJoined(
            domainOccurrencePointer, domainOption.getColumnName(), descendantIdFieldVariable);

    // COUNT(DISTINCT co.person_id) AS count
    var countField =
        new FieldVariable(
            new FieldPointer(
                domainOccurrencePointer, CriteriaQueryBuilder.PERSON_ID_FIELD_NAME, "COUNT"),
            domainOccurenceTableVariable,
            "count",
            true);

    // domain clause filters for the given domain id based on field domain_id
    // c.domain_id = 'domain'
    var domainClause =
        createDomainClause(conceptTablePointer, conceptTableVariable, domainOption.getName());

    // c.concept_name, c.concept_id, COUNT(DISTINCT co.person_id) AS count
    List<SelectExpression> select = List.of(nameField, idField, countField);

    List<TableVariable> tables =
        List.of(conceptTableVariable, conceptAncestorTableVariable, domainOccurenceTableVariable);

    // ORDER BY count DESC
    List<OrderByVariable> orderBy =
        List.of(new OrderByVariable(countField, OrderByDirection.DESCENDING));

    // GROUP BY c.concept_name, c.concept_id
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
