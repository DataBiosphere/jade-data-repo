package bio.terra.app.configuration;

import bio.terra.app.logging.LoggerInterceptor;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.springframework.web.servlet.resource.TransformedResource;

@Component
public class WebConfig implements WebMvcConfigurer {
  private final LoggerInterceptor loggerInterceptor;
  private Map<String, String> cachedJs = Collections.synchronizedMap(Map.of());

  @Autowired
  public WebConfig(LoggerInterceptor loggerInterceptor) {
    this.loggerInterceptor = loggerInterceptor;
  }

  @Override
  public void addInterceptors(InterceptorRegistry registry) {
    registry.addInterceptor(loggerInterceptor);
  }

  @Override
  public void addResourceHandlers(ResourceHandlerRegistry registry) {
    // Make swagger UI respect the x-tokenName security scheme extension because:
    // Note:
    // https://github.com/swagger-api/swagger-ui/blob/cc408812fc927e265da158bf68239530740ab4cc/src/core/oauth2-authorize.js#L6  bug in swagger UI
    addJsFixers(
        registry,
        "/webjars/springfox-swagger-ui/swagger-ui-bundle.js",
        "\"response_type=token\"",
        "\"response_type=\"+(s.get(\"x-tokenName\")||\"token\")+"
            + "\"&nonce=defaultNonce&prompt=login\"");
  }

  private void addJsFixers(
      ResourceHandlerRegistry registry, String jsFile, String find, String replace) {
    String springUiJsPath = String.format("%s*", jsFile);
    if (!registry.hasMappingForPattern(springUiJsPath)) {
      registry
          .addResourceHandler(springUiJsPath)
          .setCachePeriod(3600)
          .addResourceLocations(String.format("classpath:/META-INF/resources%s", jsFile))
          .resourceChain(true)
          .addTransformer(
              (request, resource, transformerChain) -> {
                String newJs = cachedJs.computeIfAbsent(jsFile, key -> {
                  try (InputStream stream = resource.getInputStream()) {
                    return new String(stream.readAllBytes(), StandardCharsets.UTF_8)
                            .replace(find, replace);
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                });
                return new TransformedResource(resource, newJs.getBytes(StandardCharsets.UTF_8));
              });
    }
  }
}
