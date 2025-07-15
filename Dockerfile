FROM ubuntu:22.04

# Install Java 17
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless && \
    rm -rf /var/lib/apt/lists/*

# Create app directory and storage directory
WORKDIR /app
RUN mkdir -p /app/storage/impressions && chmod 755 /app/storage/impressions

# Copy your JAR file and resource files
COPY GenerateSyntheticData_deploy.jar ./app.jar
COPY 360m_population_spec.textproto ./
COPY 90day_1billion_data_spec.textproto ./

# Run the application
CMD ["java", "-jar", "app.jar", \
     "--event-group-reference-id=event-group-reference-id/edpa-eg-reference-id-1", \
     "--population-spec-resource-path=360m_population_spec.textproto", \
     "--data-spec-resource-path=90day_1billion_data_spec.textproto", \
     "--output-bucket=impressions", \
     "--local-storage-path=/app/storage", \
     "--kms-type=NONE"]
