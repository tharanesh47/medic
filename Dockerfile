# 1. Use an official Maven image as a base
FROM maven:3.8.6-openjdk-11-slim AS builder

# 2. Set the working directory inside the container
WORKDIR /app

# 3. Copy the pom.xml and download the dependencies (this improves caching)
COPY pom.xml .

# 4. Run Maven to download dependencies and build the application
RUN mvn clean install -DskipTests

# 5. Copy the rest of the source code
COPY src /app/src

# 6. Build the application (use the proper Maven build command)
RUN mvn package -DskipTests

# 7. Use a new base image for running the application (lighter image)
FROM openjdk:11-jre-slim

# 8. Set the working directory for the runtime container (use /app instead of /target)
WORKDIR /app

# 9. Copy the built JAR from the builder stage into the runtime container
COPY --from=builder /app/target/Patient_Data.jar /app/Patient_Data.jar

# 10. Expose the port the app will be running on (optional, depending on the app)
EXPOSE 8080

# 11. Command to run the JAR file
CMD ["java", "-jar", "/app/Patient_Data.jar"]
