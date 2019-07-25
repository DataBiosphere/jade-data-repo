package bio.terra.service;

import bio.terra.category.Unit;
import bio.terra.model.JobModel;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Stairway;
import bio.terra.stairway.exception.FlightNotFoundException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;

@RunWith(SpringRunner.class)
@AutoConfigureMockMvc
@SpringBootTest
@Category(Unit.class)
public class JobServiceTest {
    @Autowired
    private Stairway stairway;

    @Autowired
    private JobService jobService;

    @Test
    public void retrieveTest() throws Exception {
        // We perform 7 flights and then retrieve and enumerate them.
        // The fids list should be in exactly the same order as the database ordered by submit time.

        List<String> fids = new ArrayList<>();
        for (int i = 0; i < 7; i++) {
            String fid = runFlight(makeDescription(i));
            fids.add(fid);
        }

        // Test single retrieval
        testSingleRetrieval(fids);

        // Test result retrieval - the body should be the description string
        testResultRetrieval(fids);

        // Retrieve everything
        testEnumRange(fids, 0, 100);

        // Retrieve the middle 3; offset means skip 2 rows
        testEnumRange(fids, 2, 3);

        // Retrieve from the end; should only get the last one back
        testEnumCount(1, 6, 3);

        // Retrieve past the end; should get nothing
        testEnumCount(0, 22, 3);
    }

    private void testSingleRetrieval(List<String> fids) {
        ResponseEntity<JobModel> response = jobService.retrieveJob(fids.get(2));
        Assert.assertThat(response.getStatusCode(), is(equalTo(HttpStatus.OK)));
        JobModel job3 = response.getBody();
        Assert.assertNotNull(job3);
        validateJobModel(job3, 2, fids);
    }

    private void testResultRetrieval(List<String> fids) {
        ResponseEntity<Object> result = jobService.retrieveJobResultResponse(fids.get(2));
        Assert.assertThat(result.getStatusCode(), is(equalTo(HttpStatus.I_AM_A_TEAPOT)));
        String resultDesc = (String) result.getBody();
        Assert.assertThat(resultDesc, is(equalTo(makeDescription(2))));
    }

    // Get some range and compare it with the fids
    private void testEnumRange(List<String> fids, int offset, int limit) {
        ResponseEntity<List<JobModel>> response = jobService.enumerateJobs(offset, limit);
        Assert.assertThat(response.getStatusCode(), is(equalTo(HttpStatus.OK)));
        List<JobModel> jobList = response.getBody();
        Assert.assertNotNull(jobList);
        int index = offset;
        for (JobModel job : jobList) {
            validateJobModel(job, index, fids);
            index++;
        }
    }

    // Get some range and make sure we got the number we expected
    private void testEnumCount(int count, int offset, int length) {
        ResponseEntity<List<JobModel>> response = jobService.enumerateJobs(offset, length);
        Assert.assertThat(response.getStatusCode(), is(equalTo(HttpStatus.OK)));
        List<JobModel> jobList = response.getBody();
        Assert.assertNotNull(jobList);
        Assert.assertThat(jobList.size(), is(count));
    }

    @Test(expected = FlightNotFoundException.class)
    public void testBadIdRetrieveJob() {
        jobService.retrieveJob("abcdef");
    }

    @Test(expected = FlightNotFoundException.class)
    public void testBadIdRetrieveResult() {
        jobService.retrieveJobResultResponse("abcdef");
    }

    private void validateJobModel(JobModel jm, int index, List<String> fids) {
        Assert.assertThat(jm.getDescription(), is(equalTo(makeDescription(index))));
        Assert.assertThat(jm.getId(), is(equalTo(fids.get(index))));
        Assert.assertThat(jm.getJobStatus(), is(JobModel.JobStatusEnum.SUCCEEDED));
        Assert.assertThat(jm.getStatusCode(), is(HttpStatus.I_AM_A_TEAPOT.value()));
    }

    // Submit a flight; wait for it to finish; return the flight id
    private String runFlight(String description) {
        FlightMap inputs = new FlightMap();
        inputs.put(JobMapKeys.DESCRIPTION.getKeyName(), description);

        String flightId = stairway.submit(JobServiceTestFlight.class, inputs);
        stairway.waitForFlight(flightId);
        return flightId;
    }

    private String makeDescription(int ii) {
        return String.format("flight%d", ii);
    }

}
