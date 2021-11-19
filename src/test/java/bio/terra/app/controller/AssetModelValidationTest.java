package bio.terra.app.controller;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import bio.terra.common.TestUtils;
import bio.terra.common.category.Unit;
import bio.terra.model.AssetModel;
import bio.terra.model.ErrorModel;
import java.util.List;
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
public class AssetModelValidationTest {

  @Autowired private MockMvc mvc;

  private ErrorModel expectBadAssetModel(AssetModel asset) throws Exception {
    MvcResult result =
        mvc.perform(
                post("/api/repository/v1/datasets/assets")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(TestUtils.mapToJson(asset)))
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
  public void testInvalidAssetRequest() throws Exception {
    mvc.perform(
            post("/api/repository/v1/datasets/assets")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{}"))
        .andExpect(status().is4xxClientError());
  }

  private void checkValidationErrorModel(
      String context, ErrorModel errorModel, String[] messageCodes) {
    List<String> details = errorModel.getErrorDetail();
    int requiredDetailSize = messageCodes.length;
    assertThat("Got the expected error details", details.size(), equalTo(requiredDetailSize));
    assertThat(
        "Main message is right",
        errorModel.getMessage(),
        containsString("Validation errors - see error details"));
    for (int i = 0; i < messageCodes.length; i++) {
      assertThat(
          context + ": correct message code (" + i + ")",
          /**
           * The global exception handler logs in this format:
           *
           * <p><fieldName>: '<messageCode>' (<defaultMessage>)
           *
           * <p>We check to see if the code is wrapped in quotes to prevent matching on substrings.
           */
          details.get(i),
          containsString("'" + messageCodes[i] + "'"));
    }
  }
}
