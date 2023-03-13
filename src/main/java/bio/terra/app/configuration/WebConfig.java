package bio.terra.app.configuration;

import bio.terra.app.logging.LoggerInterceptor;
import bio.terra.app.usermetrics.UserMetricsInterceptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Component
public class WebConfig implements WebMvcConfigurer {
  @Autowired private LoggerInterceptor loggerInterceptor;
  @Autowired private UserMetricsInterceptor metricsInterceptor;

  @Override
  public void addInterceptors(InterceptorRegistry registry) {
    registry.addInterceptor(loggerInterceptor);
    registry.addInterceptor(metricsInterceptor);
  }

  @Override
  public void addResourceHandlers(ResourceHandlerRegistry registry) {
    registry
        .addResourceHandler("/webjars/swagger-ui-dist/**")
        .addResourceLocations("classpath:/META-INF/resources/webjars/swagger-ui-dist/4.3.0/");
  }

  @Override
  public void addCorsMappings(CorsRegistry registry) {
    registry
        .addMapping("/**")
        .allowedOrigins("*")
        .allowedMethods("GET", "POST", "PUT", "DELETE", "HEAD");
  }
}
