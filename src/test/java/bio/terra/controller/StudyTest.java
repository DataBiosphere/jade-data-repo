package bio.terra.controller;

import bio.terra.category.Unit;
import bio.terra.controller.exception.ApiException;
import bio.terra.dao.StudyDao;
import bio.terra.dao.exception.StudyNotFoundException;
import bio.terra.flight.study.create.StudyCreateFlight;
import bio.terra.metadata.Study;
import bio.terra.model.AssetModel;
import bio.terra.model.AssetTableModel;
import bio.terra.model.ColumnModel;
import bio.terra.model.RelationshipModel;
import bio.terra.model.RelationshipTermModel;
import bio.terra.model.StudyJsonConversion;
import bio.terra.model.StudyModel;
import bio.terra.model.StudyRequestModel;
import bio.terra.model.StudySpecificationModel;
import bio.terra.model.TableModel;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.FlightState;
import bio.terra.stairway.Stairway;
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
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;


import bio.terra.fixtures.FlightStates;


@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Unit.class)
public class StudyTest {

    @MockBean
    private Stairway stairway;

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

    private static final String testFlightId = "test-flight-id";

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

    }

    private void expectBadStudyCreateRequest(StudyRequestModel studyRequest) throws Exception {
        mvc.perform(post("/api/repository/v1/studies")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(studyRequest)))
                .andExpect(status().is4xxClientError());
    }

    @Test
    public void testMinimalCreate() throws Exception {
        FlightState flightState = FlightStates.makeFlightSimpleState();

        when(stairway.submit(eq(StudyCreateFlight.class), isA(FlightMap.class)))
                .thenReturn(testFlightId);
        // Call to mocked waitForFlight will do nothing, so no need to handle that
        when(stairway.getFlightState(eq(testFlightId)))
                .thenReturn(flightState);

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
        FlightState flightState = FlightStates.makeFlightSimpleState();

        when(stairway.submit(eq(StudyCreateFlight.class), isA(FlightMap.class)))
                .thenReturn(testFlightId);
        // Call to mocked waitForFlight will do nothing, so no need to handle that
        when(stairway.getFlightState(eq(testFlightId)))
                .thenReturn(flightState);

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
    public void testInvalidStudyRequest() throws Exception {
        mvc.perform(post("/api/repository/v1/studies")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{}"))
                .andExpect(status().is4xxClientError());
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
                .isRoot(true)
                .columns(Collections.emptyList());

        AssetModel asset = new AssetModel()
                .name("bad_asset")
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

    @MockBean
    private StudyDao studyDao;


    @Test
    public void testStudyRetrieve() throws Exception {
        assertThat("Study retrieve with bad id gets 400",
                mvc.perform(get("/api/repository/v1/studies/{id}", "blah"))
                        .andReturn().getResponse().getStatus(),
                equalTo(HttpStatus.BAD_REQUEST.value()));

        UUID missingId = UUID.fromString("cd100f94-e2c6-4d0c-aaf4-9be6651276a6");
        when(studyDao.retrieve(eq(missingId))).thenThrow(
                new StudyNotFoundException("Study not found for id " + missingId.toString()));
        assertThat("Study retrieve that doesn't exist returns 404",
                mvc.perform(get("/api/repository/v1/studies/{id}", missingId))
                        .andReturn().getResponse().getStatus(),
                equalTo(HttpStatus.NOT_FOUND.value()));

        UUID id = UUID.fromString("8d2e052c-e1d1-4a29-88ed-26920907791f");
        Study study = StudyJsonConversion.studyRequestToStudy(studyRequest);
        study.setId(id).setCreatedDate(Instant.now());

        when(studyDao.retrieve(eq(id))).thenReturn(study);
        assertThat("Study retrieve returns 200",
                mvc.perform(get("/api/repository/v1/studies/{id}", id.toString()))
                        .andReturn().getResponse().getStatus(),
                equalTo(HttpStatus.OK.value()));

        mvc.perform(get("/api/repository/v1/studies/{id}", id.toString())).andDo((result) ->
                        assertThat("Study retrieve returns a Study Model with schema",
                                objectMapper.readValue(result.getResponse().getContentAsString(), StudyModel.class)
                                        .getName(),
                                equalTo(studyRequest.getName())));

        assertThat("Study retrieve returns a Study Model with schema",
                objectMapper.readValue(
                        mvc.perform(get("/api/repository/v1/studies/{id}", id))
                                .andReturn()
                                .getResponse()
                                .getContentAsString(),
                        StudyModel.class).getName(),
                equalTo(studyRequest.getName()));
    }

}
