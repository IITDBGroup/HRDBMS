########################################
# Dockerfile to build image iitdbgroup/hrdbms-deploy:latest
########################################
FROM iitdbgroup/hrdbms:latest

########################################
# Let HRDBMS user own huffman.dat
########################################
RUN chown hrdbms /home/hrdbms/huffman.dat \
    && chgrp hrdbms /home/hrdbms/huffman.dat

########################################
# Install packages needed for building
########################################
RUN apt-get -y update && apt-get install -y \
	unzip \
	&& rm -rf /var/lib/apt/lists/*
########################################
# Download and build and deploy HRDBMS
########################################
USER hrdbms
RUN wget -O /home/hrdbms/hrdbms.zip https://github.com/IITDBGroup/HRDBMS/archive/merged.zip \
    && cd /home/hrdbms \
	&& unzip hrdbms.zip \
	&& rm hrdbms.zip

RUN cd /home/hrdbms/HRDBMS-merged \
    && ant -f HRDBMS/build.xml createJar \
	&& mv /home/hrdbms/HRDBMS-merged/HRDBMS/build/HRDBMS.jar /home/hrdbms/ \
	&& cp /home/hrdbms/HRDBMS-merged/HRDBMS/huffman.dat /home/hrdbms/ \
	&& rm -rf /home/hrdbms/HRDBMS-merged

########################################
# Run ssh demon per default
########################################
USER root
CMD ["/usr/sbin/sshd", "-D"]
