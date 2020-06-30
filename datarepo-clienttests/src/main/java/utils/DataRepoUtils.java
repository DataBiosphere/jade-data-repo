package utils;

import bio.terra.datarepo.api.RepositoryApi;
import bio.terra.datarepo.model.JobModel;
import com.fasterxml.jackson.databind.ObjectMapper;

public final class DataRepoUtils {

    private DataRepoUtils() { }

    private static int maximumSecondsToWaitForJob = 500;
    private static int secondsIntervalToPollJob = 5;

    public static JobModel waitForJobToFinish(RepositoryApi repositoryApi, JobModel job) throws Exception {
        int pollCtr = Math.floorDiv(maximumSecondsToWaitForJob, secondsIntervalToPollJob);
        job = repositoryApi.retrieveJob(job.getId());

        while (job.getJobStatus().equals(JobModel.JobStatusEnum.RUNNING) && pollCtr >= 0) {
            job = repositoryApi.retrieveJob(job.getId());
            pollCtr--;
            Thread.sleep(secondsIntervalToPollJob);
        }

        if (job.getJobStatus().equals(JobModel.JobStatusEnum.RUNNING)) {
            throw new RuntimeException("Timed out waiting for job to finish. (jobid=" + job.getId() + ")");
        }

        return job;
    }

    public static <T> T getJobResult(RepositoryApi repositoryApi, JobModel job, Class<T> resultClass) throws Exception {
        Object jobResult = repositoryApi.retrieveJobResult(job.getId());

        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.convertValue(jobResult, resultClass);
    }
}
