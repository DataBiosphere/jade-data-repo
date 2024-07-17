package bio.terra.service.snapshotbuilder.utils;

import static bio.terra.service.snapshotbuilder.utils.QueryBuilderFactory.FILTER_TEXT;

import bio.terra.common.exception.InternalServerErrorException;
import bio.terra.model.SnapshotBuilderColumn;
import bio.terra.model.SnapshotBuilderColumnFilterDetails;
import bio.terra.model.SnapshotBuilderColumnSelectDetails;
import bio.terra.model.SnapshotBuilderDomainOption;
import bio.terra.model.SnapshotBuilderJoinModel;
import bio.terra.model.SnapshotBuilderPrimaryTable;
import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.FilterVariable;
import bio.terra.service.snapshotbuilder.query.Literal;
import bio.terra.service.snapshotbuilder.query.OrderByDirection;
import bio.terra.service.snapshotbuilder.query.OrderByVariable;
import bio.terra.service.snapshotbuilder.query.Query;
import bio.terra.service.snapshotbuilder.query.SelectExpression;
import bio.terra.service.snapshotbuilder.query.SubstituteVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BinaryFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BooleanAndOrFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.FunctionFilterVariable;
import bio.terra.service.snapshotbuilder.query.table.Concept;
import bio.terra.service.snapshotbuilder.query.table.Table;
import java.util.List;
import java.util.Objects;

public class EnumerateConceptsQueryBuilder {

  /**
   * Generate a query that retrieves all the concepts from the given searched text. If text is not
   * provided, all concepts in the domain will be enumerated.
   *
   * <pre>{@code
   *  GCP: filterClause = (CONTAINS_SUBSTR(c.concept_name, 'filter_text') OR CONTAINS_SUBSTR(c.concept_code, 'filter_text'))
   *  Azure: filterClause = CHARINDEX('search_text', c.concept_name) > 0 OR CHARINDEX('filter_text', c.concept_code) > 0))
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
  public Query buildEnumerateConceptsQuery(
      SnapshotBuilderDomainOption domainOption, boolean hasFilterText) {
    // Initialize query values
    List<Table> tables = new java.util.ArrayList<>(List.of());
    List<SelectExpression> select = new java.util.ArrayList<>(List.of());
    List<FilterVariable> searchTermFilterVariables = new java.util.ArrayList<>(List.of());
    List<FilterVariable> totalFilterVariables = new java.util.ArrayList<>(List.of());
    List<OrderByVariable> orderBy = new java.util.ArrayList<>(List.of());
    List<FieldVariable> groupBy = new java.util.ArrayList<>(List.of());

    // Add the primary table to the lists
    SnapshotBuilderPrimaryTable snapshotBuilderPrimaryTable =
        domainOption.getEnumerate().getStartsWith();
    Table primaryTable = Table.asPrimary(snapshotBuilderPrimaryTable.getName());
    tables.add(primaryTable);
    addColumnInformation(
        snapshotBuilderPrimaryTable.getColumnsUsed(),
        primaryTable,
        select,
        searchTermFilterVariables,
        totalFilterVariables,
        orderBy,
        groupBy);

    Objects.requireNonNullElse(
            domainOption.getEnumerate().getJoinTables(), List.<SnapshotBuilderJoinModel>of())
        .forEach(
            snapshotBuilderJoinModel -> {
              Table joinTable =
                  Table.asJoined(
                      snapshotBuilderJoinModel.getTo().getTable(),
                      snapshotBuilderJoinModel.getTo().getColumn(),
                      tables.stream()
                          .filter(
                              table ->
                                  Objects.equals(
                                      table.tableName(),
                                      snapshotBuilderJoinModel.getFrom().getTable()))
                          .findFirst()
                          .orElseThrow(
                              () ->
                                  new InternalServerErrorException(
                                      "Snapshot builder settings malformed"))
                          .getFieldVariable(snapshotBuilderJoinModel.getFrom().getColumn()),
                      snapshotBuilderJoinModel.isLeftJoin());
              tables.add(joinTable);
              addColumnInformation(
                  snapshotBuilderJoinModel.getTo().getColumnsUsed(),
                  joinTable,
                  select,
                  searchTermFilterVariables,
                  totalFilterVariables,
                  orderBy,
                  groupBy);
            });

    if (hasFilterText && searchTermFilterVariables.size() > 0) {
      totalFilterVariables.add(
          new BooleanAndOrFilterVariable(
              BooleanAndOrFilterVariable.LogicalOperator.OR, searchTermFilterVariables));
    }

    FilterVariable where =
        new BooleanAndOrFilterVariable(
            BooleanAndOrFilterVariable.LogicalOperator.AND, totalFilterVariables);

    return new Query.Builder()
        .select(select)
        .tables(tables)
        .where(where)
        .groupBy(groupBy)
        .orderBy(orderBy)
        .limit(100)
        .build();
  }

  public void addColumnInformation(
      List<SnapshotBuilderColumn> columns,
      Table table,
      List<SelectExpression> select,
      List<FilterVariable> searchTermFilterVariables,
      List<FilterVariable> totalFilterVariables,
      List<OrderByVariable> orderBy,
      List<FieldVariable> groupBy) {
    columns.forEach(
        snapshotBuilderColumn -> {
          SnapshotBuilderColumnSelectDetails selectDetails =
              snapshotBuilderColumn.getSelectDetails();
          FieldVariable fieldVariable;
          if (snapshotBuilderColumn.getSelectDetails() != null) {
            fieldVariable =
                table.getFieldVariable(
                    snapshotBuilderColumn.getName(),
                    selectDetails.getFunctionWrapper(),
                    selectDetails.getAlias(),
                    selectDetails.isIsDistinct());
            select.add(fieldVariable);
            if (snapshotBuilderColumn.isIsGroupBy() != null
                && snapshotBuilderColumn.isIsGroupBy()) {
              groupBy.add(fieldVariable);
            }
            if (snapshotBuilderColumn.getOrderByDetails() != null) {
              switch (snapshotBuilderColumn.getOrderByDetails()) {
                case ASCENDING -> orderBy.add(new OrderByVariable(fieldVariable));
                case DESCENDING -> orderBy.add(
                    new OrderByVariable(fieldVariable, OrderByDirection.DESCENDING));
              }
            }
          } else {
            fieldVariable = table.getFieldVariable(snapshotBuilderColumn.getName());
          }
          List<SnapshotBuilderColumnFilterDetails> filterDetails =
              snapshotBuilderColumn.getFilterDetails();
          if (snapshotBuilderColumn.getFilterDetails() != null) {
            FieldVariable filterFieldVariable = fieldVariable;
            filterDetails.forEach(
                filterDetail -> {
                  switch (filterDetail.getType()) {
                    case LITERAL -> totalFilterVariables.add(
                        BinaryFilterVariable.equals(
                            filterFieldVariable, new Literal(filterDetail.getLiteral())));
                    case SEARCH_TERM -> searchTermFilterVariables.add(
                        createFilterConceptClause(filterFieldVariable));
                  }
                });
          }
        });
  }

  static FunctionFilterVariable createFilterConceptClause(FieldVariable fieldVariable) {
    return new FunctionFilterVariable(
        FunctionFilterVariable.FunctionTemplate.TEXT_EXACT_MATCH,
        fieldVariable,
        new SubstituteVariable(FILTER_TEXT));
  }

  static FilterVariable createDomainClause(Concept concept, String domainId) {
    return BooleanAndOrFilterVariable.and(
        BinaryFilterVariable.equals(concept.domainId(), new Literal(domainId)),
        BinaryFilterVariable.equals(concept.standardConcept(), new Literal("S")));
  }
}
