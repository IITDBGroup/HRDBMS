#!/bin/bash

# Install new ant version
mkdir -p /usr/share/ant && cd
wget -q http://archive.apache.org/dist/ant/binaries/apache-ant-${ANT_VERSION}-bin.tar.gz
tar -xzf apache-ant-${ANT_VERSION}-bin.tar.gz
mv apache-ant-${ANT_VERSION}/ /usr/share/ant
rm apache-ant-${ANT_VERSION}-bin.tar.gz
ln -s /usr/share/ant/apache-ant-${ANT_VERSION}/bin/ant /usr/bin/ant
