package bio.terra.service.iam.sam;

import org.broadinstitute.dsde.workbench.client.sam.ApiException;

// We provide our own functional interface so that we can declare the `throws ApiException`
// That way, the exception handling can be done once in the retry perform method instead of in each
// Inner method in SamIam.

@FunctionalInterface
public interface SamVoidFunction {
    void apply() throws ApiException, InterruptedException;
}
