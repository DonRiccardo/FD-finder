FROM maven:4.0.0-rc-4-eclipse-temurin-21 AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn clean package

FROM openjdk:21
WORKDIR /tmp
ARG JAR_FILE=target/DataService-0.0.1-SNAPSHOT.jar
COPY --from=build /app/${JAR_FILE} app.jar
ENTRYPOINT ["java","-jar","app.jar"]