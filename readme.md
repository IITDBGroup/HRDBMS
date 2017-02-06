# Overview
HRDBMS is a relational database for analytics/complex queries that is designed to handle large amounts of data and run across large clusters.
The nodes in an HRDBMS cluster are divided into 2 types: coordinators nodes and workers nodes.  The majority of the cluster will be made of worker nodes.  These are the nodes that will actually store table data and do the majority of query processing.  The remaining nodes are coordinator nodes.  The coordinator nodes are responsible for accepting incoming requests from JDBC clients (no ODBC supported as of yet) and doing query planning and optimization.  Your cluster must have at least 1 coordinator and 3 workers.  You can have multiple coordinators if you choose.  The main advantage of this is to be able to handle more clients, and (in the future) to provide HA for coordinators.

The [Wiki](https://github.com/IITDBGroup/HRDBMS/wiki) covers how to build, install, configure, and start HRDBMS as well as discusses SQL syntax and how to use the JDBC driver and CLI interface.

# HRDBMS Wiki #

## Main ##

* [Home][home]

## Installation ##

* [Build and Deployment][build]

## Usage ##

* [Command Line Client][cli]
* [JDBC Driver][jdbc]
* [SQL dialect][sql]
* [Configuration file][config]
* [Docker deployment][docker]

## Research ##

* [HRDBMS Research Webpage](http://www.cs.iit.edu/%7edbgroup/research/hrdbms.php)

[home]: https://github.com/IITDBGroup/HRDBMS/wiki/Home
[build]: https://github.com/IITDBGroup/HRDBMS/wiki/build
[sql]: https://github.com/IITDBGroup/HRDBMS/wiki/sql_dialect
[cli]: https://github.com/IITDBGroup/HRDBMS/wiki/cli
[jdbc]: https://github.com/IITDBGroup/HRDBMS/wiki/jdbc
[config]: https://github.com/IITDBGroup/HRDBMS/wiki/config_file
[docker]: https://github.com/IITDBGroup/HRDBMS/wiki/docker

# Contact Info

If we start to get more users, I'll start to prioritize things based on what the users need with the understanding that my research needs have to come first.  Please use the issues feature on github to submit bug reports and feature requests.  I'll respond to this much better than things getting lost in email.  But if you need to go over something with me that requires some discussion, you can reach me at jasonar81@gmail.com.  Just put HRDBMS in the subject line.  Thanks!

