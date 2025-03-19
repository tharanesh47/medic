
FROM amazoncorretto:21
COPY target/Patient_Data-1.0.jar /Patient_Data/
COPY target/Patient_Data/Patient_Data-*/lib/* /Patient_Data/lib/
WORKDIR /Patient_Data
RUN mv Patient_Data-*.jar Patient_Data.jar
ENTRYPOINT ["java", "-3
Xms1g", "-Xmx2g", "-jar", "Patient_Data.jar"]