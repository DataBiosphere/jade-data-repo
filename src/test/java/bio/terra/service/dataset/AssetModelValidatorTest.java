package bio.terra.service.dataset;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import bio.terra.common.TestUtils;
import bio.terra.common.category.Unit;
import bio.terra.model.AssetModel;
import bio.terra.model.AssetTableModel;
import bio.terra.model.ErrorModel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.hamcrest.Matcher;
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

  private ErrorModel expectBadAssetCreateRequest(AssetModel assetModel) throws Exception {
    MvcResult result =
        mvc.perform(
                post("/api/repository/v1/datasets/" + UUID.randomUUID() + "/assets")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(TestUtils.mapToJson(assetModel)))
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
  public void testInvalidColumnAssetCreateRequest() throws Exception {
    List<AssetTableModel> tables = new ArrayList<>();
    List<String> columns = new ArrayList<>();
    columns.add("column1");
    tables.add(new AssetTableModel().name("Test").columns(columns));
    ErrorModel errorModel =
        expectBadAssetCreateRequest(
            new AssetModel()
                .name("TestInvalidColumn")
                .tables(tables)
                .rootTable("rootTableName")
                .rootColumn("rootColumnName"));
    checkValidationErrorModel(
        errorModel, new String[] {"NotNull", "NotNull", "NotNull", "AssetNameMissing"});
  }

  private void checkValidationErrorModel(ErrorModel errorModel, String[] messageCodes) {
    List<String> details = errorModel.getErrorDetail();
    assertThat(
        "Main message is right",
        errorModel.getMessage(),
        containsString("Validation errors - see error details"));
    /*
     * The global exception handler logs in this format:
     *
     * <fieldName>: '<messageCode>' (<defaultMessage>)
     *
     * We check to see if the code is wrapped in quotes to prevent matching on substrings.
     */
    List<Matcher<? super String>> expectedMatches =
        Arrays.stream(messageCodes)
            .map(code -> containsString("'" + code + "'"))
            .collect(Collectors.toList());
    assertThat("Detail codes are right", details, containsInAnyOrder(expectedMatches));
  }
}
