package bio.terra.app.controller;

import bio.terra.category.Unit;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.ErrorModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.util.Arrays;
import java.util.List;

import static bio.terra.common.fixtures.DatasetFixtures.buildAsset;
import static bio.terra.common.fixtures.DatasetFixtures.buildDatasetRequest;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Unit.class)
public class AssetModelValidationTest {

    @Autowired
    private MockMvc mvc;

    @Autowired
    private ObjectMapper objectMapper;

    private ErrorModel expectBadDatasetCreateRequest(DatasetRequestModel datasetRequest) throws Exception {
        MvcResult result = mvc.perform(post("/api/repository/v1/datasets/assets")
            .contentType(MediaType.APPLICATION_JSON)
            .content(objectMapper.writeValueAsString(datasetRequest)))
            .andExpect(status().is4xxClientError())
            .andReturn();

        MockHttpServletResponse response = result.getResponse();
        String responseBody = response.getContentAsString();

        assertTrue("Error model was returned on failure",
            StringUtils.contains(responseBody, "message"));

        ErrorModel errorModel = objectMapper.readValue(responseBody, ErrorModel.class);
        return errorModel;
    }

    @Test
    public void testInvalidAssetRequest() throws Exception {
        mvc.perform(post("/api/repository/v1/datasets/assets")
            .contentType(MediaType.APPLICATION_JSON)
            .content("{}"))
            .andExpect(status().is4xxClientError());
    }

    @Test
    public void testDuplicateAssetNames() throws Exception {
        DatasetRequestModel req = buildDatasetRequest();
        req.getSchema().assets(Arrays.asList(buildAsset(), buildAsset()));
        ErrorModel errorModel = expectBadDatasetCreateRequest(req);
        checkValidationErrorModel("duplicateAssetNames", errorModel,
            new String[]{"DuplicateAssetNames"});
    }

    @Test
    public void testAssetNameMissing() throws Exception {
        ErrorModel errorModel = expectBadDatasetCreateRequest(buildDatasetRequest().name(null));
        checkValidationErrorModel("assetNameMissing", errorModel,
            new String[]{"NotNull", "AssetNameMissing"});
    }

    private void checkValidationErrorModel(String context,
                                           ErrorModel errorModel,
                                           String[] messageCodes) {
        List<String> details = errorModel.getErrorDetail();
        int requiredDetailSize = messageCodes.length;
        assertThat("Got the expected error details", details.size(), equalTo(requiredDetailSize));
        assertThat("Main message is right",
            errorModel.getMessage(), containsString("Validation errors - see error details"));
        for (int i = 0; i < messageCodes.length; i++) {
            String code = messageCodes[i];
            assertThat(context + ": correct message code (" + i + ")",
                /**
                 * The global exception handler logs in this format:
                 *
                 * <fieldName>: '<messageCode>' (<defaultMessage>)
                 *
                 * We check to see if the code is wrapped in quotes to prevent matching on substrings.
                 */
                details.get(i), containsString("'" + messageCodes[i] + "'"));
        }
    }
}
