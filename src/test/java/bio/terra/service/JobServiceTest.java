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
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;

@RunWith(SpringRunner.class)
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
        ResponseEntity<JobModel> response3 = jobService.retrieveJob(fids.get(2));
        Assert.assertThat(response3.getStatusCode(), is(equalTo(HttpStatus.OK)));
        JobModel job3 = response3.getBody();
        validateJobModel(job3, 2, fids);

        // Test result retrieval - the body should be the description string
        ResponseEntity<Object> result = jobService.retrieveJobResult(fids.get(2));
        Assert.assertThat(result.getStatusCode(), is(equalTo(HttpStatus.OK)));
        String resultDesc = (String)result.getBody();
        Assert.assertThat(resultDesc, is(equalTo(makeDescription(2))));

        // Retrieve everything
        ResponseEntity<List<JobModel>> responseAll = jobService.enumerateJobs(0, 100);
        Assert.assertThat(responseAll.getStatusCode(), is(equalTo(HttpStatus.OK)));
        List<JobModel> jobList = responseAll.getBody();
        int index = 0;
        for (JobModel job : jobList) {
            validateJobModel(job, index, fids);
            index++;
        }

        // Retrieve the middle 3; offset means skip 2 rows
        ResponseEntity<List<JobModel>> responseMid = jobService.enumerateJobs(2, 3);
        Assert.assertThat(responseMid.getStatusCode(), is(equalTo(HttpStatus.OK)));
        jobList = responseMid.getBody();
        index = 2;
        for (JobModel job : jobList) {
            validateJobModel(job, index, fids);
            index++;
        }

        // Retrieve from the end; should only get the last one back
        ResponseEntity<List<JobModel>> responseLast = jobService.enumerateJobs(6, 3);
        Assert.assertThat(responseMid.getStatusCode(), is(equalTo(HttpStatus.OK)));
        jobList = responseLast.getBody();
        Assert.assertThat(jobList.size(), is(1));
        validateJobModel(jobList.get(0), 6, fids);

        // Retrieve past the end; should get nothing
        ResponseEntity<List<JobModel>> responseNone = jobService.enumerateJobs(22, 3);
        Assert.assertThat(responseMid.getStatusCode(), is(equalTo(HttpStatus.OK)));
        jobList = responseLast.getBody();
        Assert.assertThat(jobList.size(), is(0));
    }

    @Test(expected = FlightNotFoundException.class)
    public void testBadIdRetrieveJob() {
        ResponseEntity<JobModel> badResponse = jobService.retrieveJob("abcdef");
    }

    @Test(expected = FlightNotFoundException.class)
    public void testBadIdRetrieveResult() {
        ResponseEntity<Object> result = jobService.retrieveJobResult("abcdef");
    }

    private void validateJobModel(JobModel jm, int index, List<String> fids) {
        Assert.assertThat(jm.getDescription(), is(equalTo(makeDescription(index))));
        Assert.assertThat(jm.getId(), is(equalTo(fids.get(index))));
        Assert.assertThat(jm.getJobStatus(), is(equalTo(JobModel.JobStatusEnum.SUCCEEDED)));
        Assert.assertThat(jm.getStatusCode(), is(equalTo(HttpStatus.OK)));
    }

    // Submit a flight; wait for it to finish; return the flight id
    private String runFlight(String description) {
        FlightMap inputs = new FlightMap();
        inputs.put(JobMapKeys.DESCRIPTION.getKeyName(), description);

        String flightId = stairway.submit(JobServiceTestFlight.class, inputs);
        stairway.waitForFlight(flightId);
        stairway.releaseFlight(flightId);
        return flightId;
    }

    private String makeDescription(int ii) {
        return String.format("flight%d", ii);
    }

}
