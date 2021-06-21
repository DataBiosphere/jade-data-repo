package bio.terra.app.model;

/**
 * Azure cloud resources used by TDR.
 */
public enum AzureCloudResource implements CloudResource {
    APPLICATION_DEPLOYMENT("application_deployment"),
    STORAGE_ACCOUNT("storage_account"),
    SYNAPSE_WORKSPACE("synapse_workspace");

    private final String value;

    AzureCloudResource(String value) {
        this.value = value;
    }

    public String toString() {
        return value;
    }

    @Override
    public String getValue() {
        return value;
    }

    public static AzureCloudResource fromValue(String text) {
        for (AzureCloudResource resource : AzureCloudResource.values()) {
            if (resource.value.equalsIgnoreCase(text)) {
                return resource;
            }
        }
        return null;
    }
}
