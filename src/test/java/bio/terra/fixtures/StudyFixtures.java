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
import java.util.UUID;


public final class StudyFixtures {
    private StudyFixtures() {
    }

    public static StudySummaryModel buildMinimalStudySummary() {
        return new StudySummaryModel()
            .id("Minimal")
            .name("Minimal")
            .description("This is a sample study definition");
    }

    public static TableModel buildParticipantTable() {
        return new TableModel()
            .name("participant")
            .columns(Arrays.asList(
                new ColumnModel().name("id").datatype("string"),
                new ColumnModel().name("age").datatype("integer")));
    }

    public static TableModel buildSampleTable() {
        return new TableModel()
            .name("sample")
            .columns(Arrays.asList(
                new ColumnModel().name("id").datatype("string"),
                new ColumnModel().name("participant_id").datatype("string"),
                new ColumnModel().name("date_collected").datatype("date")));
    }

    public static RelationshipTermModel buildParticipantTerm() {
        return new RelationshipTermModel()
            .table("participant")
            .column("id")
            .cardinality(RelationshipTermModel.CardinalityEnum.ONE);
    }

    public static RelationshipTermModel buildSampleTerm() {
        return new RelationshipTermModel()
            .table("sample")
            .column("participant_id")
            .cardinality(RelationshipTermModel.CardinalityEnum.MANY);
    }

    public static RelationshipModel buildParticipantSampleRelationship() {
        return new RelationshipModel()
            .name("participant_sample")
            .from(buildParticipantTerm())
            .to(buildSampleTerm());
    }

    public static AssetTableModel buildAssetParticipantTable() {
        return new AssetTableModel()
            .name("participant")
            .columns(Collections.emptyList());
    }

    public static AssetTableModel buildAssetSampleTable() {
        return new AssetTableModel()
            .name("sample")
            .columns(Arrays.asList("participant_id", "date_collected"));
    }

    public static AssetModel buildAsset() {
        return new AssetModel()
            .name("sample")
            .rootTable("sample")
            .rootColumn("participant_id")
            .tables(Arrays.asList(buildAssetParticipantTable(), buildAssetSampleTable()))
            .follow(Collections.singletonList("participant_sample"));
    }

    public static StudySpecificationModel buildSchema()  {
        return new StudySpecificationModel()
            .tables(Arrays.asList(buildParticipantTable(), buildSampleTable()))
            .relationships(Collections.singletonList(buildParticipantSampleRelationship()))
            .assets(Collections.singletonList(buildAsset()));
    }

    public static StudyRequestModel buildStudyRequest() {
        return new StudyRequestModel()
            .name("Minimal")
            .description("This is a sample study definition")
            .defaultProfileId(UUID.randomUUID().toString())
            .schema(buildSchema());
    }
}
