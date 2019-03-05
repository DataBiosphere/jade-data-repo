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

import static bio.terra.fixtures.StudyFixtures.buildAsset;
import static bio.terra.fixtures.StudyFixtures.buildAssetParticipantTable;
import static bio.terra.fixtures.StudyFixtures.buildAssetSampleTable;
import static bio.terra.fixtures.StudyFixtures.buildParticipantSampleRelationship;
import static bio.terra.fixtures.StudyFixtures.buildSampleTerm;
import static bio.terra.fixtures.StudyFixtures.buildStudyRequest;
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
            .content(objectMapper.writeValueAsString(buildStudyRequest())))
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

        StudyRequestModel req = buildStudyRequest();
        req.getSchema().tables(Arrays.asList(table, table));
        expectBadStudyCreateRequest(req);
    }

    @Test
    public void testDuplicateColumnNames() throws Exception {
        ColumnModel column = new ColumnModel().name("id").datatype("string");
        TableModel table = new TableModel()
            .name("table")
            .columns(Arrays.asList(column, column));

        StudyRequestModel req = buildStudyRequest();
        req.getSchema().tables(Collections.singletonList(table));
        expectBadStudyCreateRequest(req);
    }

    @Test
    public void testMissingAssets() throws Exception {
        StudyRequestModel req = buildStudyRequest();
        req.getSchema().assets(null);
        expectBadStudyCreateRequest(req);

        req.getSchema().assets(Collections.emptyList());
        expectBadStudyCreateRequest(req);
    }

    @Test
    public void testDuplicateAssetNames() throws Exception {
        StudyRequestModel req = buildStudyRequest();
        req.getSchema().assets(Arrays.asList(buildAsset(), buildAsset()));
        expectBadStudyCreateRequest(req);
    }

    @Test
    public void testDuplicateRelationshipNames() throws Exception {
        StudyRequestModel req = buildStudyRequest();
        RelationshipModel relationship = buildParticipantSampleRelationship();
        req.getSchema().relationships(Arrays.asList(relationship, relationship));
        expectBadStudyCreateRequest(req);
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

        StudyRequestModel req = buildStudyRequest();
        req.getSchema().assets(Collections.singletonList(asset));
        expectBadStudyCreateRequest(req);
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

        StudyRequestModel req = buildStudyRequest();
        req.getSchema().assets(Collections.singletonList(asset));
        expectBadStudyCreateRequest(req);
    }

    @Test
    public void testInvalidFollowsRelationship() throws Exception {
        AssetModel asset = new AssetModel()
            .name("bad_follows")
            .tables(Arrays.asList(buildAssetSampleTable(), buildAssetParticipantTable()))
            .follow(Collections.singletonList("missing"));

        StudyRequestModel req = buildStudyRequest();
        req.getSchema().assets(Collections.singletonList(asset));
        expectBadStudyCreateRequest(req);
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
            .to(buildSampleTerm());

        StudyRequestModel req = buildStudyRequest();
        req.getSchema().relationships(Collections.singletonList(mismatchedRelationship));
        expectBadStudyCreateRequest(req);
    }


    @Test
    public void testNoRootTable() throws Exception {
        AssetModel noRoot = new AssetModel()
            .name("bad")
            // In the fixtures, the participant asset table has isRoot set to false.
            .tables(Collections.singletonList(buildAssetParticipantTable()))
            .follow(Collections.singletonList("participant_sample"));
        StudyRequestModel req = buildStudyRequest();
        req.getSchema().assets(Collections.singletonList(noRoot));
        expectBadStudyCreateRequest(req);
    }

    @Test
    public void testStudyNameInvalid() throws Exception {
        expectBadStudyCreateRequest(buildStudyRequest().name("no spaces"));
        expectBadStudyCreateRequest(buildStudyRequest().name("no-dashes"));
        expectBadStudyCreateRequest(buildStudyRequest().name(""));

        // Make a 64 character string, it should be considered too long by the validation.
        String tooLong = StringUtils.repeat("a", 64);
        expectBadStudyCreateRequest(buildStudyRequest().name(tooLong));
    }

    @Test
    public void testStudyNameMissing() throws Exception {
        expectBadStudyCreateRequest(buildStudyRequest().name(null));
    }

    @Test
    public void testStudyEnumerateValidations() throws Exception {
        expectBadStudyEnumerateRequest(-1, 3);
        expectBadStudyEnumerateRequest(0, 1);

        mvc.perform(get("/api/repository/v1/studies/")
            .contentType(MediaType.APPLICATION_JSON)
            .content(objectMapper.writeValueAsString(buildStudyRequest())))
            .andExpect(status().isOk());
    }

}
