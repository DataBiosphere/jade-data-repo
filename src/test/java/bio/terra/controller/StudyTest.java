package bio.terra.controller;

import bio.terra.category.Unit;
import bio.terra.controller.exception.ApiException;
import bio.terra.flight.study.create.StudyCreateFlight;
import bio.terra.model.AssetModel;
import bio.terra.model.AssetTableModel;
import bio.terra.model.ColumnModel;
import bio.terra.model.RelationshipModel;
import bio.terra.model.RelationshipTermModel;
import bio.terra.model.StudyRequestModel;
import bio.terra.model.StudySpecificationModel;
import bio.terra.model.StudySummaryModel;
import bio.terra.model.TableModel;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.FlightState;
import bio.terra.stairway.FlightStatus;
import bio.terra.stairway.Stairway;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
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

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

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

    @Autowired
    private MockMvc mvc;

    @Autowired
    private ObjectMapper objectMapper;

    private StudyRequestModel minimalStudyRequest;
    private StudySummaryModel minimalStudySummary;

    private static final String testFlightId = "test-flight-id";

    @Before
    public void setup() {
        List<RelationshipModel> relationships = Arrays.asList(
                new RelationshipModel()
                        .name("participant_sample")
                        .from(new RelationshipTermModel()
                                .table("participant")
                                .column("id")
                                .cardinality(RelationshipTermModel.CardinalityEnum.ONE))
                        .to(new RelationshipTermModel()
                                .table("sample")
                                .column("participant_id")
                                .cardinality(RelationshipTermModel.CardinalityEnum.MANY)));
        List<TableModel> studyTables = Arrays.asList(
                new TableModel()
                        .name("participant")
                        .columns(Arrays.asList(
                                new ColumnModel().name("id").datatype("string"),
                                new ColumnModel().name("age").datatype("number"))),
                new TableModel()
                        .name("sample")
                        .columns(Arrays.asList(
                                new ColumnModel().name("id").datatype("string"),
                                new ColumnModel().name("participant_id").datatype("string"),
                                new ColumnModel().name("date_collected").datatype("date"))));
        List<AssetModel> assets = Arrays.asList(
                new AssetModel()
                        .name("Sample")
                        .tables(Arrays.asList(
                                new AssetTableModel()
                                        .name("sample")
                                        .isRoot(true),
                                new AssetTableModel()
                                        .name("participant")))
                        .follow(Arrays.asList("participant_sample")));
        minimalStudyRequest = new StudyRequestModel()
                .name("Minimal")
                .description("This is a sample study definition")
                .schema(new StudySpecificationModel()
                        .tables(studyTables)
                        .relationships(relationships)
                        .assets(assets));
        minimalStudySummary = new StudySummaryModel()
                .name("Minimal")
                .description("This is a sample study definition");
    }

    @Test
    public void testMinimalCreate() throws Exception {
        FlightState flightState = makeFlightState();

        when(stairway.submit(eq(StudyCreateFlight.class), isA(FlightMap.class)))
                .thenReturn(testFlightId);
        // Call to mocked waitForFlight will do nothing, so no need to handle that
        when(stairway.getFlightState(eq(testFlightId)))
                .thenReturn(flightState);

        mvc.perform(post("/api/repository/v1/studies")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(minimalStudyRequest)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.name").value("Minimal"))
                .andExpect(jsonPath("$.description")
                        .value("This is a sample study definition"));
    }

    @Test
    public void testMinimalJsonCreate() throws Exception {
        FlightState flightState = makeFlightState();

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
                .content(objectMapper.writeValueAsString(minimalStudyRequest)))
                .andExpect(status().isInternalServerError());
    }

    @Test
    public void testDuplicateTableNames() throws Exception {
        List<ColumnModel> columns = Arrays.asList(new ColumnModel().name("id").datatype("string"));
        minimalStudyRequest.getSchema().tables(Arrays.asList(
                new TableModel()
                        .name("duplicate")
                        .columns(columns),
                new TableModel()
                        .name("duplicate")
                        .columns(columns)));
        mvc.perform(post("/api/repository/v1/studies")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(minimalStudyRequest)))
                .andExpect(status().is4xxClientError());
    }

    @Test
    public void testDuplicateColumnNames() throws Exception {
        List<ColumnModel> columns = Arrays.asList(
                new ColumnModel().name("id").datatype("string"),
                new ColumnModel().name("id").datatype("string"));
        minimalStudyRequest.getSchema().tables(Arrays.asList(
                new TableModel().name("table").columns(columns)));
        mvc.perform(post("/api/repository/v1/studies")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(minimalStudyRequest)))
                .andExpect(status().is4xxClientError());
    }

    private FlightState makeFlightState() {
        // Construct a mock FlightState
        FlightMap resultMap = new FlightMap();
        resultMap.put("response", minimalStudySummary);

        FlightState flightState = new FlightState();
        flightState.setFlightId(testFlightId);
        flightState.setFlightStatus(FlightStatus.SUCCESS);
        flightState.setSubmitted(Timestamp.from(Instant.now()));
        flightState.setInputParameters(resultMap); // unused
        flightState.setResultMap(Optional.of(resultMap));
        flightState.setCompleted(Optional.of(Timestamp.from(Instant.now())));
        flightState.setErrorMessage(Optional.empty());
        return flightState;
    }

}
