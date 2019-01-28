package bio.terra.controller;

import bio.terra.model.*;
import bio.terra.service.AsyncService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Arrays;
import java.util.List;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
public class StudyTest {

    @MockBean
    private AsyncService asyncService;

    @Autowired
    private MockMvc mvc;

    @Autowired
    private ObjectMapper objectMapper;

    private StudyRequestModel minimalStudyRequest;
    private StudySummaryModel minimalStudySummary;

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
        when(asyncService.submitJob(eq("create-study"), isA(StudyRequestModel.class)))
                .thenReturn("job-id");
        when(asyncService.waitForJob(eq("job-id"), eq(StudySummaryModel.class)))
                .thenReturn(minimalStudySummary);
        mvc.perform(post("/api/repository/v1/studies")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(minimalStudyRequest)))
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

    // test posting study-minimal.json is okay
    // test objectMapper to string == minimal study json
    //
    // structural
    //    table names are distinct and valid
    //    column names within a table are distinct and valid
    //
    // requires db
    //   duplicate study name test

}
