FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /build
COPY pom.xml .
COPY src/main src/main
RUN --mount=type=cache,target=/root/.m2 mvn -B -ntp -Dmaven.test.skip=true package

FROM eclipse-temurin:21-jre-jammy
WORKDIR /app
RUN groupadd --system billing && useradd --system --gid billing billing
COPY --from=build --chown=billing:billing /build/target/billing-event-pipeline-1.0.0.jar app.jar
USER billing
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
