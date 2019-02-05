package bio.terra.metadata;

import bio.terra.model.RelationshipModel;
import bio.terra.model.RelationshipTermModel;

import java.util.Map;
import java.util.UUID;

public class StudyRelationship {

    private UUID id;
    private String name;
    private StudyTableColumn from;
    private RelationshipTermModel.CardinalityEnum fromCardinality;
    private StudyTableColumn to;
    private RelationshipTermModel.CardinalityEnum toCardinality;

    public StudyRelationship() {}

    public StudyRelationship(RelationshipModel relationshipModel, Map<String, StudyTable> tables) {
        name = relationshipModel.getName();
        from = getColumn(relationshipModel.getFrom(), tables);
        fromCardinality = relationshipModel.getFrom().getCardinality();
        to = getColumn(relationshipModel.getTo(), tables);
        toCardinality = relationshipModel.getTo().getCardinality();
    }

    protected StudyTableColumn getColumn(RelationshipTermModel relTerm, Map<String, StudyTable> tables) {
        return tables.get(relTerm.getTable()).getColumnsMap().get(relTerm.getColumn());
    }

    public UUID getId() {
        return id;
    }

    public StudyRelationship setId(UUID id) { this.id = id; return this; }

    public String getName() { return name; }

    public StudyRelationship setName(String name) {
        this.name = name;
        return this;
    }

    public StudyRelationship setFrom(StudyTableColumn from) {
        this.from = from;
        return this;
    }

    public StudyRelationship setFromCardinality(RelationshipTermModel.CardinalityEnum fromCardinality) {
        this.fromCardinality = fromCardinality;
        return this;
    }

    public StudyRelationship setTo(StudyTableColumn to) {
        this.to = to;
        return this;
    }

    public StudyRelationship setToCardinality(RelationshipTermModel.CardinalityEnum toCardinality) {
        this.toCardinality = toCardinality;
        return this;
    }

    public StudyTableColumn getFrom() { return from; }

    public StudyTableColumn getTo() { return to; }

    public RelationshipTermModel.CardinalityEnum getFromCardinality() { return fromCardinality; }

    public RelationshipTermModel.CardinalityEnum getToCardinality() { return toCardinality; }
}
