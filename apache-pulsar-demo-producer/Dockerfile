FROM openjdk:17

EXPOSE 7070

ARG JAR_FILE

ADD target/${JAR_FILE} apache-pulsar-demo-producer.jar

ENTRYPOINT ["java","-Djava.security.egd=file:/dev/./urandom","-jar","/apache-pulsar-demo-producer.jar"]