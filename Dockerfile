FROM pyflink/playgrounds:1.13.0-rc2

# Copy additional JARs
COPY ./flink-libraries /opt/flink/lib/

ENTRYPOINT ["/docker-entrypoint.sh"]