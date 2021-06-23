package bio.terra.service.filedata.google.firestore;

import bio.terra.common.category.Unit;
import bio.terra.service.filedata.exception.FileSystemExecutionException;
import bio.terra.service.resourcemanagement.google.GoogleResourceConfiguration;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

@Category(Unit.class)
public class BatchOperationTest {
    private FireStoreUtils fireStoreUtils;

    @Before
    public void setup() {
        GoogleResourceConfiguration resourceConfiguration = new GoogleResourceConfiguration();
        resourceConfiguration.setFirestoreRetries(4);
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
