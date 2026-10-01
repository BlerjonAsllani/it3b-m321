# Mehrstufig: zuerst mit Maven bauen, dann nur das fertige JAR in ein kleines Laufzeit-Image
# kopieren. So landet weder Maven noch der Quelltext im Bild, das spaeter laeuft.
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build

# Der Eltern-POM und der POM des anderen Moduls muessen mit, sonst kennt Maven den Verbund
# nicht. Gebaut wird trotzdem nur der chat-service (-pl) samt Eltern-POM (-am).
COPY pom.xml .
COPY batch-writer/pom.xml batch-writer/pom.xml
COPY chat-service chat-service
RUN mvn -q -pl chat-service -am package -DskipTests

# Zweite Stufe: nur die Java-Laufzeit, kein Maven, kein Quelltext.
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /build/chat-service/target/chat-service-0.1.0.jar app.jar

# Absichtlich kein EXPOSE und kein veroeffentlichter Port: der Dienst ist nur innerhalb des
# Docker-Netzes chat-net erreichbar.
ENTRYPOINT ["java", "-jar", "app.jar"]
