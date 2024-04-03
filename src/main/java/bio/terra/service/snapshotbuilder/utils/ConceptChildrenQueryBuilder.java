package bio.terra.service.snapshotbuilder.utils;

import bio.terra.model.SnapshotBuilderDomainOption;
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
import bio.terra.service.snapshotbuilder.query.filtervariable.SubQueryFilterVariable;
import java.util.List;

public class ConceptChildrenQueryBuilder {

  ConceptChildrenQueryBuilder(TableNameGenerator tableNameGenerator) {
    this.tableNameGenerator = tableNameGenerator;
  }

  //  public static String buildConceptChildrenQuery(
  //      int parentConceptId, TableNameGenerator tableNameGenerator, CloudPlatformWrapper platform)
  // {
  //    TableVariable concept =
  //        TableVariable.forPrimary(TablePointer.fromTableName("concept", tableNameGenerator));
  //    FieldVariable conceptName = concept.makeFieldVariable("concept_name");
  //    FieldVariable conceptId = concept.makeFieldVariable("concept_id");
  //
  //    TableVariable conceptRelationship =
  //        TableVariable.forPrimary(
  //            TablePointer.fromTableName("concept_relationship", tableNameGenerator));
  //    FieldVariable descendantConceptId = conceptRelationship.makeFieldVariable("concept_id_2");
  //
  //    Query selectAllDescendants =
  //        new Query(
  //            List.of(descendantConceptId),
  //            List.of(conceptRelationship),
  //            BooleanAndOrFilterVariable.and(
  //                BinaryFilterVariable.equals(
  //                    conceptRelationship.makeFieldVariable("concept_id_1"),
  //                    new Literal(parentConceptId)),
  //                BinaryFilterVariable.equals(
  //                    conceptRelationship.makeFieldVariable("relationship_id"),
  //                    new Literal("Subsumes"))));
  //    /* Generates SQL like:
  //      SELECT c.concept_name, c.concept_id
  //          FROM concept AS c
  //          WHERE (c.concept_id IN
  //            (SELECT c.concept_id_2 FROM concept_relationship AS c
  //            WHERE (c.concept_id_1 = 101 AND c.relationship_id = 'Subsumes')) AND
  // c.standard_concept = 'S')
  //    */
  //    Query query =
  //        new Query(
  //            List.of(conceptName, conceptId),
  //            List.of(concept),
  //            BooleanAndOrFilterVariable.and(
  //                SubQueryFilterVariable.in(conceptId, selectAllDescendants),
  //                BinaryFilterVariable.equals(
  //                    concept.makeFieldVariable("standard_concept"), new Literal("S"))));
  //    return query.renderSQL(platform);
  private final TableNameGenerator tableNameGenerator;
  private static final String CONCEPT = "concept";
  private static final String CONCEPT_ANCESTOR = "concept_ancestor";
  private static final String PERSON_ID = "person_id";
  private static final String DOMAIN_ID = "domain_id";
  private static final String CONCEPT_ID = "concept_id";
  private static final String CONCEPT_NAME = "concept_name";
  private static final String ANCESTOR_CONCEPT_ID = "ancestor_concept_id";
  private static final String DESCENDANT_CONCEPT_ID = "descendant_concept_id";

  /**
   * Generate a query that retrieves the descendants of the given concept and their roll-up counts.
   *
   * <p>SELECT cc.concept_id, cc.concept_name, COUNT(DISTINCT co.person_id) as count FROM `concept`
   * AS cc JOIN `concept_ancestor` AS ca ON cc.concept_id = ca.ancestor_concept_id JOIN
   * `'domain'_occurrence` AS co ON co.'domain'_concept_id = ca.descendant_concept_id WHERE
   * (c.concept_id IN (SELECT c.descendant_concept_id FROM `concept_ancestor` AS c WHERE
   * c.ancestor_concept_id = conceptId AND c.descendant_concept_id != conceptId) GROUP BY
   * cc.concept_id, cc.concept_name ORDER BY cc.concept_name ASC
   */
  public Query buildConceptChildrenQuery(
      SnapshotBuilderDomainOption domainOption, int parentConceptId) {

    // concept table and its fields concept_name and concept_id
    TableVariable concept =
        TableVariable.forPrimary(TablePointer.fromTableName(CONCEPT, tableNameGenerator));
    FieldVariable conceptName = concept.makeFieldVariable(CONCEPT_NAME);
    FieldVariable conceptId = concept.makeFieldVariable(CONCEPT_ID);

    // concept_ancestor joined on concept.concept_id = ancestor_concept_id
    TableVariable conceptAncestor =
        TableVariable.forJoined(
            TablePointer.fromTableName(CONCEPT_ANCESTOR, tableNameGenerator),
            ANCESTOR_CONCEPT_ID,
            conceptId);
    FieldVariable descendantConceptId = conceptAncestor.makeFieldVariable(DESCENDANT_CONCEPT_ID);

    // domain specific occurrence table joined on concept_ancestor.descendant_concept_id =
    // 'domain'_concept_id
    TableVariable domainOccurrence =
        TableVariable.forJoined(
            TablePointer.fromTableName(domainOption.getTableName(), tableNameGenerator),
            domainOption.getColumnName(),
            descendantConceptId);

    // COUNT(DISTINCT person_id)
    FieldVariable count = domainOccurrence.makeFieldVariable(PERSON_ID, "COUNT", "count", true);

    List<SelectExpression> select = List.of(conceptName, conceptId, count);

    List<TableVariable> tables = List.of(concept, conceptAncestor, domainOccurrence);

    List<FieldVariable> groupBy = List.of(conceptName, conceptId);

    List<OrderByVariable> orderBy =
        List.of(new OrderByVariable(conceptName, OrderByDirection.ASCENDING));

    // WHERE c.concept_id IN ({createSubQuery()}) AND c.standard_concept = 'S'
    FilterVariable where =
        BooleanAndOrFilterVariable.and(
            SubQueryFilterVariable.in(
                conceptId, createSubQuery(parentConceptId, tableNameGenerator)),
            BinaryFilterVariable.equals(
                concept.makeFieldVariable("standard_concept"), new Literal("S")));

    return new Query(select, tables, where, groupBy, orderBy);
  }

  /**
   * Generate a query that retrieves the descendants of the given concept. Since concepts are listed
   * as their own descendants it excludes that case.
   *
   * <p>SELECT c.descendant_concept_id FROM `concept_ancestor` AS c WHERE c.ancestor_concept_id =
   * conceptId AND c.descendant_concept_id != conceptId
   */
  Query createSubQuery(int conceptId, TableNameGenerator tableNameGenerator) {
    // concept_relationship is primary table for the subquery
    TableVariable conceptRelationship =
        TableVariable.forPrimary(
            TablePointer.fromTableName("concept_relationship", tableNameGenerator));
    FieldVariable descendantConceptId = conceptRelationship.makeFieldVariable("concept_id_2");

    //    SELECT c.concept_id_2 FROM concept_relationship AS c
    //            WHERE (c.concept_id_1 = 101 AND c.relationship_id = 'Subsumes'))
    return new Query(
        List.of(descendantConceptId),
        List.of(conceptRelationship),
        BooleanAndOrFilterVariable.and(
            BinaryFilterVariable.equals(
                conceptRelationship.makeFieldVariable("concept_id_1"), new Literal(conceptId)),
            BinaryFilterVariable.equals(
                conceptRelationship.makeFieldVariable("relationship_id"),
                new Literal("Subsumes"))));
  }

  /**
   * Generate a query that retrieves the domain id of the concept specified by the given conceptId.
   *
   * <p>SELECT c.domain_id FROM concept AS c WHERE c.concept_id = conceptId
   */
  public Query retrieveDomainId(int conceptId) {
    TableVariable concept =
        TableVariable.forPrimary(TablePointer.fromTableName(CONCEPT, tableNameGenerator));
    FieldVariable domainIdField = concept.makeFieldVariable(DOMAIN_ID);

    BinaryFilterVariable where =
        new BinaryFilterVariable(
            concept.makeFieldVariable(CONCEPT_ID),
            BinaryFilterVariable.BinaryOperator.EQUALS,
            new Literal(conceptId));

    List<SelectExpression> select = List.of(domainIdField);
    List<TableVariable> table = List.of(concept);

    return new Query(select, table, where);
  }
}
