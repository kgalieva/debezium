# This Dockerfile is used by cdc-core to build the Debezium Image for the Shopify CDC in that buildkite pipeline
FROM gcr.io/shopify-docker-images/cloud/maven-gradle-sbt-jdk@sha256:5dcf45a3eb4eb623db447d6476ff2bdd322a260e4d977efe48da4d6ec1a34055

USER root

WORKDIR /app/src
COPY . /app/src/

# Install the components needed to build the project
RUN --mount=type=secret,id=maven_read,dst=/root/.m2/settings.xml \
    --mount=type=cache,target=/root/.m2 \
    mvn clean install -pl debezium-connector-mysql -am -Passembly -DskipITs -DskipTests && \
    mvn clean install -pl debezium-core -am -Passembly -DskipITs -DskipTests && \
    mvn clean install -pl debezium-api -am -Passembly -DskipITs -DskipTests \
