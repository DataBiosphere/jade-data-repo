package bio.terra.app.controller;

import bio.terra.category.Unit;
import bio.terra.app.controller.exception.ApiException;
import bio.terra.service.dataset.exception.DatasetNotFoundException;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.fixtures.DatasetFixtures;
import bio.terra.service.dataset.Dataset;
import bio.terra.model.AssetModel;
import bio.terra.service.dataset.DatasetJsonConversion;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.service.iam.SamClientService;
import bio.terra.service.dataset.DatasetService;
import com.fasterxml.jackson.databind.ObjectMapper;
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

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;


@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Unit.class)
public class DatasetTest {

    @Autowired
    private MockMvc mvc;

    @MockBean
    private SamClientService sam;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private JsonLoader jsonLoader;

    @MockBean
    private DatasetService datasetService;

    private static final String testFlightId = "test-flight-id";

    @Test
    public void testMinimalCreate() throws Exception {
        when(datasetService.createDataset(any(), any()))
            .thenReturn(DatasetFixtures.buildMinimalDatasetSummary());

        mvc.perform(post("/api/repository/v1/datasets")
            .contentType(MediaType.APPLICATION_JSON)
            .header("Authorization", "Bearer: faketoken")
            .content(objectMapper.writeValueAsString(DatasetFixtures.buildDatasetRequest())))
            .andExpect(status().isCreated())
            .andExpect(jsonPath("$.name").value("Minimal"))
            .andExpect(jsonPath("$.description")
                .value("This is a sample dataset definition"));
    }

    @Test
    public void testMinimalJsonCreate() throws Exception {
        when(datasetService.createDataset(any(), any()))
            .thenReturn(DatasetFixtures.buildMinimalDatasetSummary());

        String datasetJSON = jsonLoader.loadJson("dataset-minimal.json");
        mvc.perform(post("/api/repository/v1/datasets")
            .header("Authorization", "Bearer: faketoken")
            .contentType(MediaType.APPLICATION_JSON)
            .content(datasetJSON))
            .andExpect(status().isCreated())
            .andExpect(jsonPath("$.name").value("Minimal"))
            .andExpect(jsonPath("$.description")
                .value("This is a sample dataset definition"));
    }

    @Test
    public void testFlightError() throws Exception {
        when(datasetService.createDataset(any(), any())).thenThrow(ApiException.class);
        mvc.perform(post("/api/repository/v1/datasets")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(DatasetFixtures.buildDatasetRequest())))
                .andExpect(status().isInternalServerError());
    }

    @Test
    public void testDatasetRetrieve() throws Exception {
        assertThat("Dataset retrieve with bad id gets 400",
                mvc.perform(get("/api/repository/v1/datasets/{id}", "blah"))
                        .andReturn().getResponse().getStatus(),
                equalTo(HttpStatus.BAD_REQUEST.value()));

        UUID missingId = UUID.fromString("cd100f94-e2c6-4d0c-aaf4-9be6651276a6");
        when(datasetService.retrieveModel(eq(missingId))).thenThrow(
                new DatasetNotFoundException("Dataset not found for id " + missingId.toString()));
        assertThat("Dataset retrieve that doesn't exist returns 404",
                mvc.perform(get("/api/repository/v1/datasets/{id}", missingId))
                        .andReturn().getResponse().getStatus(),
                equalTo(HttpStatus.NOT_FOUND.value()));

        UUID id = UUID.fromString("8d2e052c-e1d1-4a29-88ed-26920907791f");
        DatasetRequestModel req = DatasetFixtures.buildDatasetRequest();
        Dataset dataset = DatasetJsonConversion.datasetRequestToDataset(req);
        dataset
            .id(id)
            .createdDate(Instant.now())
            .dataProjectId("foo-bar-baz");

        when(datasetService.retrieveModel(eq(id))).thenReturn(DatasetJsonConversion.datasetModelFromDataset(dataset));
        assertThat("Dataset retrieve returns 200",
                mvc.perform(get("/api/repository/v1/datasets/{id}", id.toString()))
                        .andReturn().getResponse().getStatus(),
                equalTo(HttpStatus.OK.value()));

        mvc.perform(get("/api/repository/v1/datasets/{id}", id.toString()))
            .andDo((result) -> {
                DatasetModel datasetModel =
                    objectMapper.readValue(result.getResponse().getContentAsString(), DatasetModel.class);
                assertThat("Dataset retrieve returns a Dataset Model with schema",
                    datasetModel.getName(),
                    equalTo(req.getName()));
                List<AssetModel> assets = datasetModel.getSchema().getAssets();
                assertThat("There are assets", assets.size(), greaterThan(0));
                AssetModel assetModel = assets.get(0);
                assertThat("Asset is not null", assetModel, notNullValue());
                assertThat("Asset root table is set", assetModel.getRootTable(), notNullValue());
                assertThat("Asset root column is set", assetModel.getRootColumn(), notNullValue());
            });

        assertThat("Dataset retrieve returns a Dataset Model with schema",
                objectMapper.readValue(
                        mvc.perform(get("/api/repository/v1/datasets/{id}", id))
                                .andReturn()
                                .getResponse()
                                .getContentAsString(),
                        DatasetModel.class).getName(),
                equalTo(req.getName()));
    }

}
