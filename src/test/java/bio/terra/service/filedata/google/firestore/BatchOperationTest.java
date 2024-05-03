package bio.terra.service.filedata.google.firestore;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.app.configuration.SamConfiguration;
import bio.terra.common.category.Unit;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.filedata.exception.FileSystemExecutionException;
import bio.terra.service.filedata.google.gcs.GcsConfiguration;
import bio.terra.service.resourcemanagement.google.GoogleResourceConfiguration;
import java.util.ArrayList;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(properties = {"datarepo.testWithEmbeddedDatabase=false"})
@AutoConfigureMockMvc
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class BatchOperationTest {

  @Autowired private SamConfiguration samConfiguration;
  @Autowired private GcsConfiguration gcsConfiguration;
  @Autowired private ApplicationConfiguration appConfiguration;

  private FireStoreUtils fireStoreUtils;

  @Before
  public void setup() {
    // Use fewer firestoreRetries for testing
    GoogleResourceConfiguration resourceConfiguration =
        new GoogleResourceConfiguration("jade-data-repo", 600, 4, false, "123456", "78910");
    ConfigurationService configurationService =
        new ConfigurationService(samConfiguration, resourceConfiguration, appConfiguration);

    fireStoreUtils = new FireStoreUtils(configurationService, appConfiguration);
  }

  @Test
  public void batchSuccessTest() throws Exception {
    // Make sure batch operation works without any retries
    FakeApiFuture.initialize(0); // never throw

    List<String> inputs = makeInputs(10);
    List<String> outputs = fireStoreUtils.batchOperation(inputs, input -> new FakeApiFuture());
    assertThat("correct output size", outputs.size(), equalTo(inputs.size()));
  }

  @Test
  public void batchRetrySuccessTest() throws Exception {
    // make sure batch operation works with some retries
    // 15 retries should fail entirely on the first loop, half on the second loop,
    // and succeed on the third loop.
    FakeApiFuture.initialize(15);

    List<String> inputs = makeInputs(10);
    List<String> outputs = fireStoreUtils.batchOperation(inputs, input -> new FakeApiFuture());
    assertThat("correct output size", outputs.size(), equalTo(inputs.size()));
  }

  @Test(expected = FileSystemExecutionException.class)
  public void batchFailureTest() throws Exception {
    // make sure batch operation works with some retries
    // 25 retries should fail entirely four times through and give up
    FakeApiFuture.initialize(25);
    List<String> inputs = makeInputs(5);
    fireStoreUtils.batchOperation(inputs, input -> new FakeApiFuture());
  }

  private List<String> makeInputs(int count) {
    List<String> inputs = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      inputs.add("in" + i);
    }
    return inputs;
  }
}
