package bio.terra.app.model;

import bio.terra.common.EnumUtils;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;

import java.io.IOException;

/**
 * Represents resource types across different cloud providers
 */
@JsonDeserialize(using = CloudResource.CloudResourceDeserializer.class)
public interface CloudResource {
    String name();

    class CloudResourceDeserializer extends StdDeserializer<CloudResource> {

        public CloudResourceDeserializer() {
            this(CloudResource.class);
        }
        public CloudResourceDeserializer(Class<?> vc) {
            super(vc);
        }

        @Override
        public CloudResource deserialize(
            JsonParser jsonParser,
            DeserializationContext ctxt) throws IOException {
            JsonNode node = jsonParser.getCodec().readTree(jsonParser);
            if (node.isNull()) {
                return null;
            }
            if (!node.isTextual()) {
                throw new InvalidFormatException(
                    jsonParser,
                    "Invalid representation of CloudResource",
                    node,
                    CloudResource.class);
            }
            String value = node.textValue();
            CloudResource resource = EnumUtils.valueOfLenient(GoogleCloudResource.class, value);
            if (resource != null) {
                return resource;
            }
            resource = EnumUtils.valueOfLenient(AzureCloudResource.class, value);
            if (resource != null) {
                return resource;
            }
            throw new InvalidFormatException(
                jsonParser,
                String.format("Unrecognized CloudResource: %s", value),
                node,
                CloudResource.class);
        }
    }
}
