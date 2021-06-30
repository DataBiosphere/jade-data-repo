package bio.terra.app.model;

import com.azure.resourcemanager.storage.models.SkuName;
import com.azure.resourcemanager.storage.models.StorageAccountSkuType;

/**
 * Enum describing the storage account types supported by TDR
 */
public enum AzureStorageAccountSkuType {
    STANDARD_LRS(StorageAccountSkuType.STANDARD_LRS),
    STANDARD_GRS(StorageAccountSkuType.STANDARD_GRS);

    private final StorageAccountSkuType value;

    AzureStorageAccountSkuType(StorageAccountSkuType value) {
        this.value = value;
    }

    /**
     * Returns an enum value based on the Azure external representation of the storage account type name
     * or null if not found
     */
    public static AzureStorageAccountSkuType fromAzureName(String name) {
        SkuName skuName = SkuName.fromString(name);
        for (AzureStorageAccountSkuType resource : AzureStorageAccountSkuType.values()) {
            if (resource.value.name().equals(skuName)) {
                return resource;
            }
        }
        return null;
    }

    public StorageAccountSkuType getValue() {
        return value;
    }

}
