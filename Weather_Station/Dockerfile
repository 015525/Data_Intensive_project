# Base image
FROM openjdk:11

# Set the working directory
WORKDIR /app

# Copy the application JAR file
COPY /out/artifacts/Weather_Station_jar/Weather_Station.jar ./app.jar


# Run the application
CMD ["java", "-jar", "app.jar"]

