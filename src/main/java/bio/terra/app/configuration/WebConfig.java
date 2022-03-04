package bio.terra.app.configuration;

import bio.terra.app.logging.LoggerInterceptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Component
public class WebConfig implements WebMvcConfigurer {
  private final LoggerInterceptor loggerInterceptor;

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
    registry
        .addResourceHandler("/webjars/swagger-ui-dist/**")
        .addResourceLocations("classpath:/META-INF/resources/webjars/swagger-ui-dist/4.3.0/");
  }
}
