package bio.terra.service.dataset;

import static bio.terra.service.dataset.ValidatorTestUtils.checkValidationErrorModel;
import static org.junit.Assert.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import bio.terra.common.TestUtils;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.model.ErrorModel;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

@RunWith(SpringRunner.class)
@SpringBootTest(properties = {"datarepo.testWithEmbeddedDatabase=false"})
@AutoConfigureMockMvc
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class AssetModelValidatorTest {

  @Autowired private MockMvc mvc;
  @Autowired private JsonLoader jsonLoader;

  private ErrorModel expectBadAssetCreateRequest(String jsonModel) throws Exception {
    MvcResult result =
        mvc.perform(
                post("/api/repository/v1/datasets/" + UUID.randomUUID() + "/assets")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(jsonModel))
            .andExpect(status().is4xxClientError())
            .andReturn();

    MockHttpServletResponse response = result.getResponse();
    String responseBody = response.getContentAsString();

    assertTrue(
        "Error model was returned on failure", StringUtils.contains(responseBody, "message"));

    ErrorModel errorModel = TestUtils.mapFromJson(responseBody, ErrorModel.class);
    return errorModel;
  }

  @Test
  public void testInvalidAssetCreateRequest() throws Exception {
    ErrorModel errorModel = expectBadAssetCreateRequest("{}");
    checkValidationErrorModel(errorModel, new String[] {"NotNull", "NotNull", "NotNull"});
  }

  @Test
  public void testDuplicateColumnAssetCreateRequest() throws Exception {
    String jsonModel = jsonLoader.loadJson("dataset-asset-duplicate-column.json");
    ErrorModel errorModel = expectBadAssetCreateRequest(jsonModel);
    checkValidationErrorModel(errorModel, new String[] {"DuplicateColumnNames"});
  }
}
