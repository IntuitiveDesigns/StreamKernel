FROM amazoncorretto:21-alpine
WORKDIR /app
COPY build/libs/StreamKernel-0.0.1-SNAPSHOT-all.jar app.jar
CMD ["java", "-Xms2g", "-Xmx2g", "-jar", "app.jar"]
