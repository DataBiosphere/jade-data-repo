package bio.terra.service.dataset;

import bio.terra.app.model.AzureCloudResource;
import bio.terra.app.model.AzureRegion;
import bio.terra.app.model.GoogleCloudResource;
import bio.terra.app.model.GoogleRegion;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.JsonLoader;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Unit.class)
public class StorageResourceTest {

    @Autowired
    private JsonLoader jsonLoader;

    @Autowired
    private ObjectMapper objectMapper;

    private final List<StorageResource> model = List.of(
        StorageResource.getAzureInstance()
            .cloudResource(AzureCloudResource.APPLICATION_DEPLOYMENT)
            .region(AzureRegion.CENTRAL_US)
            .datasetId(UUID.fromString("a3d54871-8cdc-4549-8410-28005df9cbaf")),
        StorageResource.getGoogleInstance()
            .cloudResource(GoogleCloudResource.BUCKET)
            .region(GoogleRegion.US_EAST1)
            .datasetId(UUID.fromString("a3d54871-8cdc-4549-8410-28005df9cbaf")));

    @Test
    public void testDeserialization() throws IOException {
        List<StorageResource> storageResource =
            jsonLoader.loadObject("storage-account.json", new TypeReference<>() {});
        assertThat(storageResource, equalTo(model));
    }

    @Test
    public void testDeserializationMixedCloudResourcesFail() throws IOException {
        assertThrows(JsonMappingException.class, () ->
            jsonLoader.loadObject("storage-account.mixedcloud.json",
                new TypeReference<List<StorageResource>>() {}),
            "mixed cloud resources don't deserialize");
    }

    @Test
    public void testSerialization() throws IOException {
        String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(model);
        System.err.println(json);
        List<StorageResource> storageResource = objectMapper.readValue(json, new TypeReference<>() {});
        assertThat(storageResource, equalTo(model));
    }
}
