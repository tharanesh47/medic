# Use OpenJDK 21
FROM openjdk:21

RUN apt-get update && apt-get install -y maven
RUN mvn clean install -DskipTests

# Create directory for the application
WORKDIR /Patient_Data

# Copy the main application JAR (renaming it directly)
COPY target/Patient_Data.jar /Patient_Data/Patient_Data.jar

# Copy dependencies (Ensure correct path)
COPY target/Patient_Data/lib/ /Patient_Data/lib/

# Run the Java application
ENTRYPOINT ["java", "-Xms1g", "-Xmx2g", "-jar", "Patient_Data.jar"]