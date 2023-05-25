# Base image
FROM openjdk:11

# Set the working directory
WORKDIR /app

# Copy the application JAR file
COPY target/kafka-consumer-1.0-SNAPSHOT.jar ./app.jar

# Copy the dependencies
COPY target/libs/* ./libs/

# Set the classpath to include the dependencies
ENV CLASSPATH="/app/libs/*:${CLASSPATH}"

# Run the application
CMD ["java", "-jar", "app.jar"]

