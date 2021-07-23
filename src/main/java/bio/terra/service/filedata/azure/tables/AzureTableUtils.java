package bio.terra.service.filedata.azure.tables;

import bio.terra.service.filedata.exception.FileSystemExecutionException;
import bio.terra.service.filedata.google.firestore.ApiFutureGenerator;
import com.azure.core.http.rest.PagedIterable;
import com.azure.data.tables.TableClient;
import com.azure.data.tables.models.ListEntitiesOptions;
import com.azure.data.tables.models.TableEntity;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class AzureTableUtils {
  private final Logger logger = LoggerFactory.getLogger(AzureTableUtils.class);
  private static final int SLEEP_BASE_SECONDS = 1;
  private static final int AZURE_STORAGE_RETRIES = 1;

  @Autowired
  public AzureTableUtils() {}

  <V> void scanTableObjects(TableClient tableClient, ApiFutureGenerator<V, TableEntity> generator) {
    ListEntitiesOptions options = new ListEntitiesOptions();
    PagedIterable<TableEntity> entities = tableClient.listEntities(options, null, null);
    entities.stream()
        .forEach(
            entity -> {
              try {
                retryOperation(entity, generator);
              } catch (InterruptedException e) {
                throw new FileSystemExecutionException("operation failed", e);
              }
            });
  }

  <T, V> void retryOperation(V input, ApiFutureGenerator<T, V> generator)
      throws InterruptedException {
    int retry = 0;
    while (true) {
      try {
        generator.accept(input).get();
        break;
      } catch (ExecutionException ex) {
        final long retryWait = (long) (SLEEP_BASE_SECONDS * Math.pow(2.5, retry));
        retry++;
        if (retry > AZURE_STORAGE_RETRIES) {
          throw new FileSystemExecutionException(
              "Operation failed after " + AZURE_STORAGE_RETRIES + " tries.");
        } else {
          logger.warn(
              "Error in Azure storage table future get - input: "
                  + input.toString()
                  + " message: "
                  + ex.getMessage());
          logger.info(
              "Operation will attempt retry #{} after {} millisecond pause.", retry, retryWait);
        }
        // Exponential backoff
        TimeUnit.SECONDS.sleep(retryWait);
      }
    }
  }
}
