package bio.terra.fixtures;

import bio.terra.model.AssetModel;
import bio.terra.model.AssetTableModel;
import bio.terra.model.ColumnModel;
import bio.terra.model.RelationshipModel;
import bio.terra.model.RelationshipTermModel;
import bio.terra.model.StudyRequestModel;
import bio.terra.model.StudySpecificationModel;
import bio.terra.model.StudySummaryModel;
import bio.terra.model.TableModel;

import java.util.Arrays;
import java.util.Collections;


public final class StudyFixtures {
    private StudyFixtures() {
    }

    public static final StudySummaryModel minimalStudySummary = new StudySummaryModel()
        .id("Minimal")
        .name("Minimal")
        .description("This is a sample study definition");

    public static final TableModel participantTable = new TableModel()
        .name("participant")
        .columns(Arrays.asList(
            new ColumnModel().name("id").datatype("string"),
            new ColumnModel().name("age").datatype("number")));

    public static final TableModel sampleTable = new TableModel()
        .name("sample")
        .columns(Arrays.asList(
            new ColumnModel().name("id").datatype("string"),
            new ColumnModel().name("participant_id").datatype("string"),
            new ColumnModel().name("date_collected").datatype("date")));

    public static final RelationshipTermModel participantTerm = new RelationshipTermModel()
        .table("participant")
        .column("id")
        .cardinality(RelationshipTermModel.CardinalityEnum.ONE);

    public static final RelationshipTermModel sampleTerm = new RelationshipTermModel()
        .table("sample")
        .column("participant_id")
        .cardinality(RelationshipTermModel.CardinalityEnum.MANY);

    public static final RelationshipModel participantSampleRelationship = new RelationshipModel()
        .name("participant_sample")
        .from(participantTerm)
        .to(sampleTerm);

    public static final AssetTableModel assetParticipantTable = new AssetTableModel()
        .name("participant")
        .columns(Collections.emptyList());

    public static final AssetTableModel assetSampleTable = new AssetTableModel()
        .name("sample")
        .columns(Arrays.asList("participant_id", "date_collected"));

    public static final AssetModel asset = new AssetModel()
        .name("Sample")
        .rootTable("sample")
        .rootColumn("participant_id")
        .tables(Arrays.asList(assetParticipantTable, assetSampleTable))
        .follow(Collections.singletonList("participant_sample"));

    public static final StudySpecificationModel schema = new StudySpecificationModel()
        .tables(Arrays.asList(participantTable, sampleTable))
        .relationships(Collections.singletonList(participantSampleRelationship))
        .assets(Collections.singletonList(asset));

    public static final StudyRequestModel studyRequest = new StudyRequestModel()
        .name("Minimal")
        .description("This is a sample study definition")
        .schema(schema);
}
