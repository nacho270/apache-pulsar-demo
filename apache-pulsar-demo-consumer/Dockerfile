FROM openjdk:17

EXPOSE 8080

ARG JAR_FILE

ADD target/${JAR_FILE} apache-pulsar-demo-consumer.jar

ENTRYPOINT ["java","-Djava.security.egd=file:/dev/./urandom","-jar","/apache-pulsar-demo-consumer.jar"]