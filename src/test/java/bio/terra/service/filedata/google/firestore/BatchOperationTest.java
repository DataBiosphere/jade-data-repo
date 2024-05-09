package bio.terra.service.filedata.google.firestore;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.app.configuration.SamConfiguration;
import bio.terra.common.category.Unit;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.filedata.exception.FileSystemExecutionException;
import bio.terra.service.resourcemanagement.google.GoogleResourceConfiguration;
import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class BatchOperationTest {

  private FireStoreUtils fireStoreUtils;

  @BeforeEach
  void setup() {
    // Use fewer firestoreRetries for testing
    GoogleResourceConfiguration resourceConfiguration =
        new GoogleResourceConfiguration("jade-data-repo", 600, 4, false, "123456", "78910");
    ApplicationConfiguration appConfiguration = mock(ApplicationConfiguration.class);
    ConfigurationService configurationService =
        new ConfigurationService(
            mock(SamConfiguration.class), resourceConfiguration, appConfiguration);

    fireStoreUtils = new FireStoreUtils(configurationService, appConfiguration);
  }

  @Test
  void batchSuccessTest() throws Exception {
    // Make sure batch operation works without any retries
    FakeApiFuture.initialize(0); // never throw

    List<String> inputs = makeInputs(10);
    List<String> outputs = fireStoreUtils.batchOperation(inputs, input -> new FakeApiFuture());
    assertThat("correct output size", outputs, hasSize(inputs.size()));
  }

  @Test
  void batchRetrySuccessTest() throws Exception {
    // make sure batch operation works with some retries
    // 15 retries should fail entirely on the first loop, half on the second loop,
    // and succeed on the third loop.
    FakeApiFuture.initialize(15);

    List<String> inputs = makeInputs(10);
    List<String> outputs = fireStoreUtils.batchOperation(inputs, input -> new FakeApiFuture());
    assertThat("correct output size", outputs, hasSize(inputs.size()));
  }

  @Test
  void batchFailureTest() {
    // make sure batch operation works with some retries
    // 25 retries should fail entirely four times through and give up
    FakeApiFuture.initialize(25);
    List<String> inputs = makeInputs(5);
    assertThrows(
        FileSystemExecutionException.class,
        () -> fireStoreUtils.batchOperation(inputs, input -> new FakeApiFuture()));
  }

  private List<String> makeInputs(int count) {
    return IntStream.range(0, count).mapToObj(i -> "in" + i).toList();
  }
}
