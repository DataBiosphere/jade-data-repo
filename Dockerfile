FROM openjdk:8-jdk-alpine
VOLUME /tmp
ARG DEPENDENCY=target/dependency
COPY ${DEPENDENCY}/BOOT-INF/lib /app/lib
COPY ${DEPENDENCY}/META-INF /app/META-INF
COPY ${DEPENDENCY}/BOOT-INF/classes /app

# Helmfile
RUN wget --timeout=15 -q -O helmfile "https://github.com/roboll/helmfile/releases/download/v${helmfile_version}/helmfile_${os}_${arch}" && \
    chmod +x helmfile && \
    mv helmfile /tools/bin && \
    helmfile --version

ENTRYPOINT ["java","-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005","-cp","app:app/lib/*","bio.terra.Main"]
