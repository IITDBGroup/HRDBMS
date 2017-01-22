# Dockerfile to build image mykh74/ant:1.9.5
FROM mykh74/java:8

# Installs Ant 
ENV ANT_VERSION 1.9.5 
RUN mkdir -p /usr/share/ant && \
    cd && \
    wget -q http://archive.apache.org/dist/ant/binaries/apache-ant-${ANT_VERSION}-bin.tar.gz && \
    tar -xzf     apache-ant-${ANT_VERSION}-bin.tar.gz && \
    mv apache-ant-${ANT_VERSION}/ /usr/share/ant && \
    rm apache-ant-${ANT_VERSION}-bin.tar.gz && \
    ln -s /usr/share/ant/apache-ant-${ANT_VERSION}/bin/ant /usr/bin/ant

ENV ANT_HOME /usr/share/ant/apache-ant-${ANT_VERSION}/ 
ENV PATH ${PATH}:/usr/share/ant/apache-ant-${ANT_VERSION}/bin

