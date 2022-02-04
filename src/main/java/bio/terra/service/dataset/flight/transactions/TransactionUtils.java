package bio.terra.service.dataset.flight.transactions;

import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import java.util.UUID;

/** Common code for steps that deal with transactions */
public class TransactionUtils {

  private TransactionUtils() {}

  public static void putTransactionId(FlightContext context, UUID transactionId) {
    FlightMap workingMap = context.getWorkingMap();
    workingMap.put(JobMapKeys.TRANSACTION_ID.getKeyName(), transactionId);
  }

  public static UUID getTransactionId(FlightContext context) {
    FlightMap workingMap = context.getWorkingMap();
    return workingMap.get(JobMapKeys.TRANSACTION_ID.getKeyName(), UUID.class);
  }
}
