# Build Datarepo image from openjdk
FROM openjdk:8-jdk-alpine

# Create an app user so our program doesn't run as root.
RUN groupadd -r app &&\
    useradd  -m -r -g app -d /home/app -s /sbin/nologin -c "Docker image user" app

# volume mount
VOLUME /tmp

# Docker Args for build
ARG DEPENDENCY=target/dependency

# Code Copy
COPY --chown=app:app ${DEPENDENCY}/BOOT-INF/lib /home/app/lib
COPY --chown=app:app ${DEPENDENCY}/META-INF /home/app/META-INF
COPY --chown=app:app ${DEPENDENCY}/BOOT-INF/classes /home/app

# Change to the app user.
USER app

# Application Entrypoint
ENTRYPOINT ["java","-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005","-cp","/home/app:/home/app/lib/*","bio.terra.Main"]
