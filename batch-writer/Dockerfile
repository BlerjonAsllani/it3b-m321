# Gleicher Aufbau wie beim chat-service: bauen mit Maven, ausfuehren mit einer reinen
# Java-Laufzeit.
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build

# Eltern-POM und der POM des anderen Moduls gehoeren zum Verbund; gebaut wird nur der
# batch-writer (-pl) samt Eltern-POM (-am).
COPY pom.xml .
COPY chat-service/pom.xml chat-service/pom.xml
COPY batch-writer batch-writer
RUN mvn -q -pl batch-writer -am package -DskipTests

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /build/batch-writer/target/batch-writer-0.1.0.jar app.jar

# Der batch-writer hat keine Weboberflaeche und damit ohnehin keinen Port nach aussen.
ENTRYPOINT ["java", "-jar", "app.jar"]
