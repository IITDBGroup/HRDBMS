# Dockerfile to build image iitdbgroup/hrdbms:latest
FROM iitdbgroup/ant:latest

## SSH server setting begin
RUN apt-get update && apt-get -y install sudo && apt-get install -y openssh-server && apt-get install -y openssh-client
RUN echo 'root:root' | chpasswd
RUN ssh-keygen -P "" -t dsa -f /etc/ssh/ssh_host_dsa_key
RUN ssh-keygen -A
RUN sed -i 's/PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config

# SSH login fix. Otherwise user is kicked off after login
RUN sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd

ENV NOTVISIBLE "in users profile"
RUN sudo echo "export VISIBLE=now" >> /etc/profile
## SSH server setting end

EXPOSE 5010
EXPOSE 5015

## Add hrdbms user with sudo rights
RUN useradd -ms /bin/bash hrdbms && echo "hrdbms:hrdbms" | chpasswd && adduser hrdbms sudo
ADD sudoers.txt /etc/sudoers
RUN chmod 440 /etc/sudoers


USER hrdbms
WORKDIR /home/hrdbms
RUN mkdir /home/hrdbms/.ssh
RUN ssh-keygen -q -t rsa -N '' -f /home/hrdbms/.ssh/id_rsa
RUN cp /home/hrdbms/.ssh/id_rsa.pub /home/hrdbms/.ssh/authorized_keys

## HRDBMS directories
RUN mkdir /home/hrdbms/data1
RUN mkdir /home/hrdbms/data2
RUN mkdir /home/hrdbms/data3
RUN mkdir /home/hrdbms/log
RUN mkdir /home/hrdbms/temp1
RUN mkdir /home/hrdbms/temp2
RUN mkdir /home/hrdbms/archive
ADD huffman.dat /home/hrdbms

USER root
CMD ["/usr/sbin/sshd", "-D"]
