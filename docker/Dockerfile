FROM maven:3.6.3-jdk-11 AS maven-build

COPY . /usr/src/
WORKDIR /usr/src
RUN mvn versions:set -DnewVersion=0.0.1
RUN mvn clean install -DskipTests

FROM gcr.io/distroless/java:11

COPY --from=maven-build /usr/src/target/mintaka-0.0.1.jar .
CMD ["mintaka-0.0.1.jar"]