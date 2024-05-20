package bio.terra.service.snapshotbuilder.utils;

import bio.terra.model.SnapshotBuilderDomainOption;
import bio.terra.service.snapshotbuilder.SelectAlias;
import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.FilterVariable;
import bio.terra.service.snapshotbuilder.query.Literal;
import bio.terra.service.snapshotbuilder.query.OrderByDirection;
import bio.terra.service.snapshotbuilder.query.OrderByVariable;
import bio.terra.service.snapshotbuilder.query.Query;
import bio.terra.service.snapshotbuilder.query.SelectExpression;
import bio.terra.service.snapshotbuilder.query.filtervariable.BinaryFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BooleanAndOrFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.FunctionFilterVariable;
import bio.terra.service.snapshotbuilder.query.tables.Concept;
import bio.terra.service.snapshotbuilder.query.tables.ConceptAncestor;
import bio.terra.service.snapshotbuilder.query.tables.DomainOccurrence;
import bio.terra.service.snapshotbuilder.query.tables.Table;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

public class SearchConceptsQueryBuilder {

  /**
   * Generate a query that retrieves all the concepts from the given searched text. If a search text
   * is not provided, a search will be made only on the domain.
   *
   * <pre>{@code
   *  GCP: searchClause = (CONTAINS_SUBSTR(c.concept_name, 'search_text') OR CONTAINS_SUBSTR(c.concept_code, 'search_text'))
   *  Azure: searchClause = CHARINDEX('search_text', c.concept_name) > 0 OR CHARINDEX('search_text', c.concept_code) > 0))
   *
   *  sql_code = c.concept_name, c_concept_id, COUNT(DISTINCT co.person_id) AS count, 1 AS has_children FROM
   * `concept` AS c JOIN `concept_ancestor` AS c0 ON c0.ancestor_concept_id = c.concept_id LEFT JOIN
   * `'domain'_occurrence` AS co ON co.'domain'_concept_id = c0.descendant_concept_id WHERE
   * (c.domain_id = 'domain' AND searchClause) GROUP BY c.concept_name, c.concept_id ORDER BY count DESC}
   *
   * GCP: `SELECT {sql_code} LIMIT 100`
   * Azure: `SELECT TOP 100 {sql_code}`
   * </pre>
   */
  public Query buildSearchConceptsQuery(
      SnapshotBuilderDomainOption domainOption, String searchText) {
    Concept concept = Concept.asPrimary();
    FieldVariable nameField = concept.name();
    FieldVariable conceptId = concept.concept_id();
    FieldVariable conceptCode = concept.code();

    // FROM 'concept' as c
    // JOIN concept_ancestor as c0 ON c0.ancestor_concept_id = c.concept_id
    ConceptAncestor conceptAncestor = ConceptAncestor.joinAncestor(conceptId);

    FieldVariable descendantId = conceptAncestor.descendant_concept_id();

    // LEFT JOIN `'domain'_occurrence as co ON 'domain_occurrence'.concept_id =
    // concept_ancestor.descendant_concept_id
    DomainOccurrence domainOccurrence =
        DomainOccurrence.leftJoinOnDescendantConcept(
            domainOption.getTableName(), domainOption.getColumnName());

    // COUNT(DISTINCT co.person_id) AS count
    var countPerson = domainOccurrence.getCountPerson();

    var domainClause = createDomainClause(concept, domainOption.getName());

    // SELECT concept_name, concept_id, concept_code, count, has_children
    List<SelectExpression> select =
        List.of(
            nameField,
            conceptId,
            conceptCode,
            countPerson,
            new SelectAlias(new Literal(true), QueryBuilderFactory.HAS_CHILDREN));

    List<Table> tables = List.of(concept, conceptAncestor, domainOccurrence);

    // ORDER BY count DESC
    List<OrderByVariable> orderBy =
        List.of(new OrderByVariable(countPerson, OrderByDirection.DESCENDING));

    // GROUP BY c.concept_name, c.concept_id, concept_code
    List<FieldVariable> groupBy = List.of(nameField, conceptId, conceptCode);

    FilterVariable where;
    if (StringUtils.isEmpty(searchText)) {
      where = domainClause;
    } else {
      // search concept name clause filters for the search text based on field concept_name
      var searchNameClause = createSearchConceptClause(searchText, concept.name());

      // search concept name clause filters for the search text based on field concept_code
      var searchCodeClause = createSearchConceptClause(searchText, concept.code());

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

    return new Query.Builder()
        .select(select)
        .tables(tables)
        .where(where)
        .groupBy(groupBy)
        .orderBy(orderBy)
        .limit(100)
        .build();
  }

  static FunctionFilterVariable createSearchConceptClause(
      String searchText, FieldVariable fieldVariable) {
    return new FunctionFilterVariable(
        FunctionFilterVariable.FunctionTemplate.TEXT_EXACT_MATCH,
        fieldVariable,
        new Literal(searchText));
  }

  static FilterVariable createDomainClause(Concept concept, String domainId) {
    return BooleanAndOrFilterVariable.and(
        BinaryFilterVariable.equals(concept.domain_id(), new Literal(domainId)),
        BinaryFilterVariable.equals(concept.standardConcept(), new Literal("S")));
  }
}
