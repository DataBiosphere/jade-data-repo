package bio.terra.service.iam.sam;

import bio.terra.common.category.Unit;
import bio.terra.service.iam.exception.IamInternalServerErrorException;
import com.google.api.client.http.HttpStatusCodes;
import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Unit.class)
public class SamRetryTest {
    private int count;

    @Before
    public void setup() {
        count = 0;
    }

    @Test(expected = IamInternalServerErrorException.class)
    public void testRetryTimeout() throws Exception {
        SamConfiguration samConfig = new SamConfiguration();
        samConfig.setOperationTimeoutSeconds(10);
        samConfig.setRetryInitialWaitSeconds(1);
        samConfig.setRetryMaximumWaitSeconds(3);

        SamRetry samRetry = new SamRetry(samConfig);
        samRetry.perform(() -> testRetryFinishInner(100));
    }

    @Test
    public void testRetryFinish() throws Exception {
        SamConfiguration samConfig = new SamConfiguration();
        samConfig.setOperationTimeoutSeconds(10);
        samConfig.setRetryInitialWaitSeconds(2);
        samConfig.setRetryMaximumWaitSeconds(5);

        SamRetry samRetry = new SamRetry(samConfig);
        samRetry.perform(() -> testRetryFinishInner(2));
    }

    // Make this "Inner" to mimic the structure of the SamIam code
    // It "fails" twice and then succeeds
    private boolean testRetryFinishInner(int failCount) throws ApiException {
        if (count < failCount) {
            count++;
            throw new ApiException(HttpStatusCodes.STATUS_CODE_SERVER_ERROR, "testing");
        }
        return true;
    }

}
