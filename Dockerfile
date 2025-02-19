#FROM openjdk:8-jre-alpine
FROM openjdk:21
#RUN apk add --update tcpdump
COPY target/Patient_Data-1.0.jar /Patient_Data/
COPY target/IgnitionOnOff/IgnitionOnOff-*/lib/* /Patient_Data/lib/
WORKDIR /Patient_Data
RUN mv Patient_Data-*.jar Patient_Data.jar
ENTRYPOINT ["java", "-Xms1g", "-Xmx2g", "-jar", "Patient_Data.jar"]