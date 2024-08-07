package bio.terra;

import com.azure.analytics.synapse.spark.SparkBatchClient;
import com.azure.analytics.synapse.spark.SparkClientBuilder;
import com.azure.analytics.synapse.spark.models.SparkBatchJob;
import com.azure.analytics.synapse.spark.models.SparkBatchJobOptions;
import com.azure.core.credential.TokenCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RunTheThing {

  public static void main(String[] args) {
    TokenCredential credential =
        new ClientSecretCredentialBuilder()
            .clientId(System.getenv("AZURE_CREDENTIALS_APPLICATIONID"))
            .tenantId(System.getenv("AZURE_CREDENTIALS_HOMETENANTID"))
            .clientSecret(System.getenv("AZURE_CREDENTIALS_SECRET"))
            .build();

    SparkBatchClient batchClient =
        new SparkClientBuilder()
            // Note, we currently store something like
            //         tdr-snps-int-east-us-ondemand.sql.azuresynapse.net
            // and instead want that value to be the base workspace url., e.g.
            // https://tdr-snps-int-east-us.dev.azuresynapse.net
            // We'll want a new config var for that but for now, hacking the URL :)

            .endpoint(
                "https://"
                    + System.getenv("AZURE_SYNAPSE_WORKSPACENAME").replace("-ondemand.sql", ".dev"))
            // This is hard coded for now but should be a config value
            .sparkPoolName("testpool")
            .credential(credential)
            .buildSparkBatchClient();

    SparkBatchJobOptions options =
        new SparkBatchJobOptions()
            .setName("test run")
            .setFile(
                // The format is:
                // abfss://<container>@<storage-account>.dfs.core.windows.net/<path>
                "abfss://exec@tdrsnpsintsaeastus.dfs.core.windows.net/synapse-spark-2.6.0-SNAPSHOT.jar")
            .setClassName("bio.terra.datarepo.io.AzParquetReader")
            .setArguments(
                List.of(
                    // Same format as above
                    "--tdr.source.path abfss://2ff51f42-647d-409c-ba4e-5c288c7d555d@tdrslqwwpfikxdzadsrfvmjt.dfs.core.windows.net/metadata/parquet/data/*",
                    // This is a container of storage account SAS token (file SAS tokens don't work)
                    // for the file above
                    "--tdr.source.sas <Sas token>"))
            .setConfiguration(Map.of("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.4"))
            .setDriverMemory("1g")
            .setDriverCores(1)
            .setExecutorMemory("1g")
            .setExecutorCores(1)
            .setExecutorCount(1);

    SparkBatchJob sparkBatchJob = batchClient.createSparkBatchJob(options);
    // Poll job
    while (!Set.of("dead", "success")
        .contains(batchClient.getSparkBatchJob(sparkBatchJob.getId()).getState())) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    SparkBatchJob finalSparkBatchJob = batchClient.getSparkBatchJob(sparkBatchJob.getId());
    System.out.println(
        "Job state: " + batchClient.getSparkBatchJob(finalSparkBatchJob.getId()).getState());

    finalSparkBatchJob.getLogLines().forEach(System.out::println);
  }
}
