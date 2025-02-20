# Use OpenJDK 21
FROM openjdk:21

# Create directory for the application
WORKDIR /Patient_Data

# Copy the main application JAR (renaming it directly)
COPY target/Patient_Data-1.0.jar /Patient_Data/Patient_Data.jar

# Copy dependencies (Ensure correct path)
COPY target/Patient_Data/lib/ /Patient_Data/lib/

# Run the Java application
ENTRYPOINT ["java", "-Xms1g", "-Xmx2g", "-jar", "Patient_Data.jar"]
