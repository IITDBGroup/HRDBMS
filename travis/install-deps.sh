#!/bin/bash

# Install new ant version
sudo mkdir -p /usr/share/ant && cd
wget -q http://archive.apache.org/dist/ant/binaries/apache-ant-${ANT_VERSION}-bin.tar.gz
tar -xzf apache-ant-${ANT_VERSION}-bin.tar.gz
sudo mv apache-ant-${ANT_VERSION}/ /usr/share/ant
sudo rm apache-ant-${ANT_VERSION}-bin.tar.gz
sudo ln -s /usr/share/ant/apache-ant-${ANT_VERSION}/bin/ant /usr/local/bin/ant
