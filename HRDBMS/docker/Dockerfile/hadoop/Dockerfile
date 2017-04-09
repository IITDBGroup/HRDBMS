# Dockerfile to build Haimage
FROM sequenceiq/hadoop-docker:2.7.1
ADD csv_type.csv /tmp
ADD csv_gen.sh /tmp
RUN chmod +x /tmp/csv_gen.sh
RUN /tmp/csv_gen.sh

