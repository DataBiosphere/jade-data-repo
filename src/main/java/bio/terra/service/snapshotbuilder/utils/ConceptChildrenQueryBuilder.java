package bio.terra.service.snapshotbuilder.utils;

import bio.terra.model.SnapshotBuilderDomainOption;
import bio.terra.service.snapshotbuilder.query.Concept;
import bio.terra.service.snapshotbuilder.query.ConceptAncestor;
import bio.terra.service.snapshotbuilder.query.ConceptRelationship;
import bio.terra.service.snapshotbuilder.query.DomainOccurrence;
import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.FilterVariable;
import bio.terra.service.snapshotbuilder.query.Literal;
import bio.terra.service.snapshotbuilder.query.OrderByDirection;
import bio.terra.service.snapshotbuilder.query.OrderByVariable;
import bio.terra.service.snapshotbuilder.query.Query;
import bio.terra.service.snapshotbuilder.query.SelectExpression;
import bio.terra.service.snapshotbuilder.query.TableVariable;
import bio.terra.service.snapshotbuilder.query.TableVariableBuilder;
import bio.terra.service.snapshotbuilder.query.filtervariable.BinaryFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BooleanAndOrFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.SubQueryFilterVariable;
import java.util.List;

public class ConceptChildrenQueryBuilder {
  /**
   * Generate a query that retrieves the descendants of the given concept and their roll-up counts.
   *
   * <p>SELECT cc.concept_id, cc.concept_name, COUNT(DISTINCT co.person_id) as count FROM `concept`
   * AS cc JOIN `concept_ancestor` AS ca ON cc.concept_id = ca.ancestor_concept_id JOIN
   * `'domain'_occurrence` AS co ON co.'domain'_concept_id = ca.descendant_concept_id WHERE
   * (c.concept_id IN (SELECT c.concept_id_2 FROM concept_relationship AS c WHERE (c.concept_id_1 =
   * 101 AND * c.relationship_id = 'Subsumes')) AND c.standard_concept = 'S') GROUP BY
   * cc.concept_id, cc.concept_name ORDER BY cc.concept_name ASC
   */
  public Query buildConceptChildrenQuery(
      SnapshotBuilderDomainOption domainOption, int parentConceptId) {

    // concept table and its fields concept_name and concept_id
    Concept concept = new Concept();
    FieldVariable conceptName = concept.name();
    FieldVariable conceptId = concept.concept_id();
    FieldVariable conceptCode = concept.code();

    // concept_ancestor joined on concept.concept_id = ancestor_concept_id.
    // We use concept_ancestor for the rollup count because we want to include counts
    // from all descendants, not just direct descendants.

    ConceptAncestor conceptAncestor =
        new ConceptAncestor(
            new TableVariableBuilder().join(ConceptAncestor.ANCESTOR_CONCEPT_ID).on(conceptId));

    FieldVariable descendantConceptId = conceptAncestor.descendant_concept_id();

    // domain specific occurrence table joined on concept_ancestor.descendant_concept_id =
    // 'domain'_concept_id
    DomainOccurrence domainOccurrence =
        new DomainOccurrence(
            new TableVariableBuilder()
                .from(domainOption.getTableName())
                .leftJoin(domainOption.getColumnName())
                .on(descendantConceptId));

    // COUNT(DISTINCT person_id)
    FieldVariable countPerson = domainOccurrence.getCountPerson();

    List<SelectExpression> select =
        List.of(
            conceptName,
            conceptId,
            conceptCode,
            countPerson,
            HierarchyQueryBuilder.hasChildrenExpression(conceptId));

    List<TableVariable> tables = List.of(concept, conceptAncestor, domainOccurrence);

    List<FieldVariable> groupBy = List.of(conceptName, conceptId, conceptCode);

    List<OrderByVariable> orderBy =
        List.of(new OrderByVariable(conceptName, OrderByDirection.ASCENDING));

    // WHERE c.concept_id IN ({createSubQuery()}) AND c.standard_concept = 'S'
    FilterVariable where =
        BooleanAndOrFilterVariable.and(
            SubQueryFilterVariable.in(conceptId, createSubQuery(parentConceptId)),
            BinaryFilterVariable.equals(concept.standard_concept(), new Literal("S")));

    return new Query.Builder()
        .select(select)
        .addTables(tables)
        .addWhere(where)
        .addGroupBy(groupBy)
        .addOrderBy(orderBy)
        .build();
  }

  /**
   * Generate a query that retrieves the descendants of the given concept. We use concept
   * relationship here because it includes only the direct descendants.
   *
   * <p>SELECT c.concept_id_2 FROM concept_relationship AS c WHERE (c.concept_id_1 = 101 AND
   * c.relationship_id = 'Subsumes'))
   */
  Query createSubQuery(int conceptId) {
    // concept_relationship is primary table for the subquery
    ConceptRelationship conceptRelationship = new ConceptRelationship();
    FieldVariable descendantConceptId = conceptRelationship.concept_id_2();

    return new Query.Builder()
        .select(List.of(descendantConceptId))
        .addTables(List.of(conceptRelationship))
        .addWhere(
            BooleanAndOrFilterVariable.and(
                BinaryFilterVariable.equals(
                    conceptRelationship.makeFieldVariable(ConceptRelationship.CONCEPT_ID_1),
                    new Literal(conceptId)),
                BinaryFilterVariable.equals(
                    conceptRelationship.makeFieldVariable(ConceptRelationship.RELATIONSHIP_ID),
                    new Literal("Subsumes"))))
        .build();
  }

  /**
   * Generate a query that retrieves the domain id of the concept specified by the given conceptId.
   *
   * <p>SELECT c.domain_id FROM concept AS c WHERE c.concept_id = conceptId
   */
  public Query retrieveDomainId(int conceptId) {
    Concept concept = new Concept();
    FieldVariable domainIdField = concept.domain_id();
    FieldVariable conceptIdField = concept.concept_id();

    BinaryFilterVariable where =
        new BinaryFilterVariable(
            conceptIdField, BinaryFilterVariable.BinaryOperator.EQUALS, new Literal(conceptId));

    List<SelectExpression> select = List.of(domainIdField);
    List<TableVariable> table = List.of(concept);

    return new Query.Builder().select(select).addTables(table).addWhere(where).build();
  }
}
