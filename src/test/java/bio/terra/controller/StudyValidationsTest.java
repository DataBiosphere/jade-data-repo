package bio.terra.controller;

import bio.terra.category.Unit;
import bio.terra.model.AssetModel;
import bio.terra.model.AssetTableModel;
import bio.terra.model.ColumnModel;
import bio.terra.model.RelationshipModel;
import bio.terra.model.RelationshipTermModel;
import bio.terra.model.StudyRequestModel;
import bio.terra.model.TableModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Arrays;
import java.util.Collections;

import static bio.terra.fixtures.StudyFixtures.asset;
import static bio.terra.fixtures.StudyFixtures.assetParticipantTable;
import static bio.terra.fixtures.StudyFixtures.assetSampleTable;
import static bio.terra.fixtures.StudyFixtures.participantSampleRelationship;
import static bio.terra.fixtures.StudyFixtures.sampleTerm;
import static bio.terra.fixtures.StudyFixtures.studyRequest;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Unit.class)
public class StudyValidationsTest {

    @Autowired
    private MockMvc mvc;

    @Autowired
    private ObjectMapper objectMapper;

    private void expectBadStudyCreateRequest(StudyRequestModel studyRequest) throws Exception {
        mvc.perform(post("/api/repository/v1/studies")
            .contentType(MediaType.APPLICATION_JSON)
            .content(objectMapper.writeValueAsString(studyRequest)))
            .andExpect(status().is4xxClientError());
    }


    private void expectBadStudyEnumerateRequest(Integer offset, Integer limit) throws Exception {
        mvc.perform(get("/api/repository/v1/studies/{offset}/{limit}", offset, limit)
            .contentType(MediaType.APPLICATION_JSON)
            .content(objectMapper.writeValueAsString(studyRequest)))
            .andExpect(status().is4xxClientError());
    }

    @Test
    public void testInvalidStudyRequest() throws Exception {
        mvc.perform(post("/api/repository/v1/studies")
            .contentType(MediaType.APPLICATION_JSON)
            .content("{}"))
            .andExpect(status().is4xxClientError());
    }

    @Test
    public void testDuplicateTableNames() throws Exception {
        ColumnModel column = new ColumnModel().name("id").datatype("string");
        TableModel table = new TableModel()
            .name("duplicate")
            .columns(Collections.singletonList(column));

        studyRequest.getSchema().tables(Arrays.asList(table, table));
        expectBadStudyCreateRequest(studyRequest);
    }

    @Test
    public void testDuplicateColumnNames() throws Exception {
        ColumnModel column = new ColumnModel().name("id").datatype("string");
        TableModel table = new TableModel()
            .name("table")
            .columns(Arrays.asList(column, column));

        studyRequest.getSchema().tables(Collections.singletonList(table));
        expectBadStudyCreateRequest(studyRequest);
    }

    @Test
    public void testMissingAssets() throws Exception {
        studyRequest.getSchema().assets(null);
        expectBadStudyCreateRequest(studyRequest);

        studyRequest.getSchema().assets(Collections.emptyList());
        expectBadStudyCreateRequest(studyRequest);
    }

    @Test
    public void testDuplicateAssetNames() throws Exception {
        studyRequest.getSchema().assets(Arrays.asList(asset, asset));
        expectBadStudyCreateRequest(studyRequest);
    }

    @Test
    public void testDuplicateRelationshipNames() throws Exception {
        studyRequest.getSchema()
            .relationships(Arrays.asList(participantSampleRelationship, participantSampleRelationship));
        expectBadStudyCreateRequest(studyRequest);
    }

    @Test
    public void testInvalidAssetTable() throws Exception {
        AssetTableModel invalidAssetTable = new AssetTableModel()
            .name("mismatched_table_name")
            .columns(Collections.emptyList());

        AssetModel asset = new AssetModel()
            .name("bad_asset")
            .rootTable("mismatched_table_name")
            .tables(Collections.singletonList(invalidAssetTable))
            .follow(Collections.singletonList("participant_sample"));

        studyRequest.getSchema().assets(Collections.singletonList(asset));
        expectBadStudyCreateRequest(studyRequest);
    }

    @Test
    public void testInvalidAssetTableColumn() throws Exception {
        // participant is a valid table but date_collected is in the sample table
        AssetTableModel invalidAssetTable = new AssetTableModel()
            .name("participant")
            .columns(Collections.singletonList("date_collected"));

        AssetModel asset = new AssetModel()
            .name("mismatched")
            .rootTable("participant")
            .rootColumn("date_collected")
            .tables(Collections.singletonList(invalidAssetTable))
            .follow(Collections.singletonList("participant_sample"));

        studyRequest.getSchema().assets(Collections.singletonList(asset));
        expectBadStudyCreateRequest(studyRequest);
    }

    @Test
    public void testInvalidFollowsRelationship() throws Exception {
        AssetModel asset = new AssetModel()
            .name("bad_follows")
            .tables(Arrays.asList(assetSampleTable, assetParticipantTable))
            .follow(Collections.singletonList("missing"));

        studyRequest.getSchema().assets(Collections.singletonList(asset));
        expectBadStudyCreateRequest(studyRequest);
    }

    @Test
    public void testInvalidRelationshipTermTableColumn() throws Exception {
        // participant_id is part of the sample table, not participant
        RelationshipTermModel mismatchedTerm = new RelationshipTermModel()
            .table("participant")
            .column("participant_id")
            .cardinality(RelationshipTermModel.CardinalityEnum.ONE);

        RelationshipModel mismatchedRelationship = new RelationshipModel()
            .name("participant_sample")
            .from(mismatchedTerm)
            .to(sampleTerm);

        studyRequest.getSchema().relationships(Collections.singletonList(mismatchedRelationship));
        expectBadStudyCreateRequest(studyRequest);
    }


    @Test
    public void testNoRootTable() throws Exception {
        AssetModel noRoot = new AssetModel()
            .name("bad")
            // In the fixtures, the participant asset table has isRoot set to false.
            .tables(Collections.singletonList(assetParticipantTable))
            .follow(Collections.singletonList("participant_sample"));
        studyRequest.getSchema().assets(Collections.singletonList(noRoot));
        expectBadStudyCreateRequest(studyRequest);
    }

    @Test
    public void testStudyNameInvalid() throws Exception {
        studyRequest.name("no spaces");
        expectBadStudyCreateRequest(studyRequest);

        studyRequest.name("no-dashes");
        expectBadStudyCreateRequest(studyRequest);

        studyRequest.name("");
        expectBadStudyCreateRequest(studyRequest);

        // Make a 64 character string, it should be considered too long by the validation.
        String tooLong = StringUtils.repeat("a", 64);
        studyRequest.name(tooLong);
        expectBadStudyCreateRequest(studyRequest);
    }

    @Test
    public void testStudyNameMissing() throws Exception {
        studyRequest.name(null);
        expectBadStudyCreateRequest(studyRequest);
    }

    @Test
    public void testStudyEnumerateValidations() throws Exception {
        expectBadStudyEnumerateRequest(-1, 3);
        expectBadStudyEnumerateRequest(0, 1);

        mvc.perform(get("/api/repository/v1/studies/")
            .contentType(MediaType.APPLICATION_JSON)
            .content(objectMapper.writeValueAsString(studyRequest)))
            .andExpect(status().isOk());
    }

}
