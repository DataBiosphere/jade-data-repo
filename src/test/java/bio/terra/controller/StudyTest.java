package bio.terra.controller;

import bio.terra.category.Unit;
import bio.terra.controller.exception.ApiException;
import bio.terra.flight.study.create.StudyCreateFlight;
import bio.terra.model.*;
import bio.terra.pdao.PrimaryDataAccess;
import bio.terra.stairway.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Arrays;
import java.util.Collections;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Unit.class)
public class StudyTest {

    @MockBean
    private Stairway stairway;

    @MockBean
    private PrimaryDataAccess pdao;

    @Autowired
    private MockMvc mvc;

    @Autowired
    private ObjectMapper objectMapper;

    private TableModel participantTable;
    private TableModel sampleTable;
    private RelationshipTermModel participantTerm;
    private RelationshipTermModel sampleTerm;
    private RelationshipModel participantSampleRelationship;
    private AssetTableModel assetParticipantTable;
    private AssetTableModel assetSampleTable;
    private AssetModel asset;
    private StudySpecificationModel schema;
    private StudyRequestModel studyRequest;
    private StudySummaryModel studySummary;

    @Before
    public void setup() {
        participantTable = new TableModel()
                .name("participant")
                .columns(Arrays.asList(
                        new ColumnModel().name("id").datatype("string"),
                        new ColumnModel().name("age").datatype("number")));

        sampleTable = new TableModel()
                .name("sample")
                .columns(Arrays.asList(
                        new ColumnModel().name("id").datatype("string"),
                        new ColumnModel().name("participant_id").datatype("string"),
                        new ColumnModel().name("date_collected").datatype("date")));

        participantTerm = new RelationshipTermModel()
                .table("participant")
                .column("id")
                .cardinality(RelationshipTermModel.CardinalityEnum.ONE);

        sampleTerm = new RelationshipTermModel()
                .table("sample")
                .column("participant_id")
                .cardinality(RelationshipTermModel.CardinalityEnum.MANY);

        participantSampleRelationship = new RelationshipModel()
                .name("participant_sample")
                .from(participantTerm)
                .to(sampleTerm);

        assetParticipantTable = new AssetTableModel()
                .name("participant")
                .columns(Collections.emptyList())
                .isRoot(false);

        assetSampleTable = new AssetTableModel()
                .name("sample")
                .columns(Arrays.asList("participant_id", "date_collected"))
                .isRoot(true);

        asset = new AssetModel()
                .name("Sample")
                .tables(Arrays.asList(assetParticipantTable, assetSampleTable))
                .follow(Collections.singletonList("participant_sample"));

        schema = new StudySpecificationModel()
                .tables(Arrays.asList(participantTable, sampleTable))
                .relationships(Collections.singletonList(participantSampleRelationship))
                .assets(Collections.singletonList(asset));

        studyRequest = new StudyRequestModel()
                .name("Minimal")
                .description("This is a sample study definition")
                .schema(schema);

        studySummary = new StudySummaryModel()
                .name("Minimal")
                .description("This is a sample study definition");

        when(pdao.studyExists(anyString())).thenReturn(false);
    }

    private void expectBadStudyCreateRequest(StudyRequestModel studyRequest) throws Exception {
        mvc.perform(post("/api/repository/v1/studies")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(studyRequest)))
                .andExpect(status().is4xxClientError());
    }

    @Test
    public void testMinimalCreate() throws Exception {
        FlightMap resultMap = new FlightMap();
        resultMap.put("response", studySummary);
        when(stairway.submit(eq(StudyCreateFlight.class), isA(FlightMap.class)))
                .thenReturn("test-flight-id");
        when(stairway.getResult(eq("test-flight-id")))
                .thenReturn(new FlightResult(StepResult.getStepResultSuccess(), resultMap));
        mvc.perform(post("/api/repository/v1/studies")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(studyRequest)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.name").value("Minimal"))
                .andExpect(jsonPath("$.description")
                        .value("This is a sample study definition"));
    }

    @Test
    public void testMinimalJsonCreate() throws Exception {
        FlightMap resultMap = new FlightMap();
        resultMap.put("response", studySummary);
        when(stairway.submit(eq(StudyCreateFlight.class), isA(FlightMap.class)))
                .thenReturn("test-flight-id");
        when(stairway.getResult(eq("test-flight-id")))
                .thenReturn(new FlightResult(StepResult.getStepResultSuccess(), resultMap));
        ClassLoader classLoader = getClass().getClassLoader();
        String studyJSON = IOUtils.toString(classLoader.getResourceAsStream("study-minimal.json"));
        mvc.perform(post("/api/repository/v1/studies")
                .contentType(MediaType.APPLICATION_JSON)
                .content(studyJSON))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.name").value("Minimal"))
                .andExpect(jsonPath("$.description")
                        .value("This is a sample study definition"));
    }

    @Test
    public void testFlightError() throws Exception {
        when(stairway.submit(eq(StudyCreateFlight.class), isA(FlightMap.class)))
                .thenThrow(ApiException.class);
        mvc.perform(post("/api/repository/v1/studies")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(studyRequest)))
                .andExpect(status().isInternalServerError());
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
                .name("bad")
                .isRoot(true)
                .columns(Collections.emptyList());

        AssetModel asset = new AssetModel()
                .name("bad")
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
                .isRoot(true)
                .columns(Collections.singletonList("date_collected"));

        AssetModel asset = new AssetModel()
                .name("mismatched")
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
       RelationshipTermModel mismatchedTerm = new RelationshipTermModel()
               .table("participant")
               .column("date_collected")
               .cardinality(RelationshipTermModel.CardinalityEnum.ONE);

       RelationshipModel mismatchedRelationship = new RelationshipModel()
               .name("participant_sample")
               .from(mismatchedTerm)
               .to(sampleTerm);

       studyRequest.getSchema().relationships(Collections.singletonList(mismatchedRelationship));
       expectBadStudyCreateRequest(studyRequest);
    }

    @Test
    public void testStudyNameInUse() throws Exception {
        when(pdao.studyExists("Minimal")).thenReturn(true);
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
}
