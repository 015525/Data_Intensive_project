# Base image
FROM openjdk:19-jdk-alpine

# Set the working directory
WORKDIR /app

# Copy the application JAR file
COPY /out/artifacts/request_jar/request.jar ./app.jar


# Run the application
CMD ["java", "-jar", "app.jar"]

