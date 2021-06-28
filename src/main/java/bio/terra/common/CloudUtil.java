package bio.terra.common;

import bio.terra.model.CloudPlatform;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * Utility methods for working across cloud providers
 */
public class CloudUtil {

    public static final CloudPlatform DEFAULT_CLOUD_PLATFORM = CloudPlatform.GCP;

    private CloudUtil() {
    }

    /**
     * Execute the code in the appropriate supplier based on the cloud platform used.
     * @param cloudPlatform The cloudPlatform to execute the code for
     * @param gcpRunnable The code to execute if the specified cloud platform is CloudPlatform.GCP
     * @param azureRunnable The code to execute if the specified cloud platform is CloudPlatform.AZURE
     */
    public static void cloudExecute(CloudPlatform cloudPlatform,
                                    Runnable gcpRunnable,
                                    Runnable azureRunnable) {
        CloudPlatform cloudPlatformToUse = Objects.requireNonNullElse(cloudPlatform, DEFAULT_CLOUD_PLATFORM);

        if (cloudPlatformToUse == CloudPlatform.GCP) {
            gcpRunnable.run();
        } else if (cloudPlatformToUse == CloudPlatform.AZURE) {
            azureRunnable.run();
        } else {
            throw unsupportedOperationException(cloudPlatformToUse);
        }

    }

    /**
     * Execute the code in the appropriate supplier based on the cloud platform used.
     * @param cloudPlatform The cloudPlatform to execute the code for
     * @param gcpSupplier The code to execute if the specified cloud platform is CloudPlatform.GCP
     * @param azureSupplier The code to execute if the specified cloud platform is CloudPlatform.AZURE
     * @param <T> The type of the resulting object for being supplied by the correct supplier
     * @return A resulting object for being supplied by the correct supplier
     */
    public static <T> T cloudExecute(CloudPlatform cloudPlatform,
                                     Supplier<T> gcpSupplier,
                                     Supplier<T> azureSupplier) {
        CloudPlatform cloudPlatformToUse = Objects.requireNonNullElse(cloudPlatform, DEFAULT_CLOUD_PLATFORM);

        if (cloudPlatformToUse == CloudPlatform.GCP) {
            return gcpSupplier.get();
        } else if (cloudPlatformToUse == CloudPlatform.AZURE) {
            return azureSupplier.get();
        } else {
            throw unsupportedOperationException(cloudPlatformToUse);
        }
    }

    private static UnsupportedOperationException unsupportedOperationException(CloudPlatform cloudPlatform) {
        return new UnsupportedOperationException(cloudPlatform + " is not a recognized Cloud Platform");
    }
}
