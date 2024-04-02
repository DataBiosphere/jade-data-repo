package bio.terra.service.snapshotbuilder.utils;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.Literal;
import bio.terra.service.snapshotbuilder.query.Query;
import bio.terra.service.snapshotbuilder.query.TableNameGenerator;
import bio.terra.service.snapshotbuilder.query.TablePointer;
import bio.terra.service.snapshotbuilder.query.TableVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BinaryFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.BooleanAndOrFilterVariable;
import bio.terra.service.snapshotbuilder.query.filtervariable.SubQueryFilterVariable;
import java.util.List;

public class ConceptChildrenQueryBuilder {

  private ConceptChildrenQueryBuilder() {}

  public static String buildConceptChildrenQuery(
      int parentConceptId, TableNameGenerator tableNameGenerator, CloudPlatformWrapper platform) {
    TableVariable concept =
        TableVariable.forPrimary(TablePointer.fromTableName("concept", tableNameGenerator));
    FieldVariable conceptName = concept.makeFieldVariable("concept_name");
    FieldVariable conceptId = concept.makeFieldVariable("concept_id");

    TableVariable conceptRelationship =
        TableVariable.forPrimary(
            TablePointer.fromTableName("concept_relationship", tableNameGenerator));
    FieldVariable descendantConceptId = conceptRelationship.makeFieldVariable("concept_id_2");

    Query selectAllDescendants =
        new Query(
            List.of(descendantConceptId),
            List.of(conceptRelationship),
            BooleanAndOrFilterVariable.and(
                BinaryFilterVariable.equals(
                    conceptRelationship.makeFieldVariable("concept_id_1"),
                    new Literal(parentConceptId)),
                BinaryFilterVariable.equals(
                    conceptRelationship.makeFieldVariable("relationship_id"),
                    new Literal("Subsumes"))));
    /* Generates SQL like:
      SELECT c.concept_name, c.concept_id
          FROM concept AS c
          WHERE (c.concept_id IN
            (SELECT c.concept_id_2 FROM concept_relationship AS c
            WHERE (c.concept_id_1 = 101 AND c.relationship_id = 'Subsumes')) AND c.standard_concept = 'S')
    */
    Query query =
        new Query(
            List.of(conceptName, conceptId),
            List.of(concept),
            BooleanAndOrFilterVariable.and(
                SubQueryFilterVariable.in(conceptId, selectAllDescendants),
                BinaryFilterVariable.equals(
                    concept.makeFieldVariable("standard_concept"), new Literal("S"))));
    return query.renderSQL(platform);
  }
}
