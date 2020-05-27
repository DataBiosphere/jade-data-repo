# Build Datarepo image from openjdk
FROM openjdk:8-jdk-alpine

# Create the home directory for the new app user.
RUN mkdir -p /home/app

# Create an app user so our program doesn't run as root.
RUN groupadd -r app &&\
    useradd -r -g app -d /home/app -s /sbin/nologin -c "Docker image user" app

# Set the home directory to our app user's home.
RUN mkdir $APP_HOME
WORKDIR $APP_HOME

# Set the home directory to our app user's home.
ENV HOME=/home/app
ENV APP_HOME=/app

# volume mount
VOLUME /tmp

# Docker Args for build
ARG DEPENDENCY=target/dependency

# Code Copy
COPY --chown=app:app ${DEPENDENCY}/BOOT-INF/lib /app/lib
COPY --chown=app:app ${DEPENDENCY}/META-INF /app/META-INF
COPY --chown=app:app ${DEPENDENCY}/BOOT-INF/classes /app

# Change to the app user.
USER app

# Application Entrypoint
ENTRYPOINT ["java","-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005","-cp","app:app/lib/*","bio.terra.Main"]
