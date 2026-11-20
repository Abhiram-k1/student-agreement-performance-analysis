FROM eclipse-temurin:17-jdk-jammy
RUN apt-get update && apt-get install -y --no-install-recommends curl python3 \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /opt/sbt \
    && curl -fL https://repo.maven.apache.org/maven2/org/scala-sbt/sbt-launch/1.10.2/sbt-launch-1.10.2.jar -o /opt/sbt/sbt-launch.jar
WORKDIR /project
COPY . .
RUN java -jar /opt/sbt/sbt-launch.jar compile
EXPOSE 4040
ENTRYPOINT ["java", "-jar", "/opt/sbt/sbt-launch.jar"]
CMD ["run"]
