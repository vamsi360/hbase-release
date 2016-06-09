Project : Hbase C++ driver 

Steps to setup development environment for building this driver

All the sources were built using the "g++ (GCC) 4.8.5 20150623 (Red Hat 4.8.5-4)" compiler, a C++11 compliant. 

The driver can connect to a Kerberized cluster. However, this was tested only against MIT Kerberos. Whether access to a kerberized cluster is needed or not, the driver depends on kerberos library. Also, the Hbase server uses SASL with GSSAPI for Kerberos authentication. So, the driver needs a SASL library too, in particular the Cyrus SASL library.

1. Install CentOS 7 in Virtual Box –with good memory allocation say, [memory 4GB HDD 20GBto40GB]
2. Download Eclipse JEE mars installer eclipse-inst-linux64
	a. Install eclipse for CPP
3. Install gnu build tools
	From a terminal, do a 'sudo yum install autoconf automake libtool curl'
4. Install the git tools 
5. [OPTIONAL if libprotobuf is not installed]
	To build the protobuf libraries, download protobuf-2.5.0 from github from https://github.com/google/protobuf/releases/tag/v2.5.0 and extract it to a folder
	From a terminal, do the following:
	a. cd protobuf-2.5.0
	b. ./autogen.sh
	c. ./configure
	d. make –j4
	e. make check
	f. [sudo] make install
	g. [sudo] ldconfig # refresh shared library cache
6. To build the poco libraries
	From a terminal, do the following:
	a. [sudo]  yum install openssl libssl-dev
	b. [sudo]  yum install libiodbc2 libiodbc2-dev
	c. Download poco sources from http://pocoproject.org/releases/poco-1.7.2/poco-1.7.2.tar.gz
	d. cd poco-x.y
	e. ./configure
	f. gmake –s
	g. [sudo] gmake –s install (may require super user privileges to install the library)
7. [OPTIONAL if libzookeper is not installed]
	To build the zookeeper libraries
	a. Download the zookeeper sources from http://apache.arvixe.com/zookeeper/zookeeper-3.4.6
	From a terminal, do the following:
	b. cd zookeeper-3.4.6/src/c
	c. ./configure
	d. make 
	e. [sudo] make install
8. [OPTIONAL if Kerberos 5 library is not installed]
	To build the Kerberos 5 library
	a. Download the Kerberos 5 sources from http://web.mit.edu/kerberos/dist/index.html#krb5-1.14
	From a terminal, do the following:
	b. cd <krb5_root_folder>/src
	c. ./configure
	d. make
	e. [sudo] make install
9. To build the Cyrus SASL library
	a. Download the Cyrus SASL sources from ftp://ftp.cyrusimap.org/cyrus-sasl/cyrus-sasl-2.1.26.tar.gz
	Extract the sources and from a terminal do the following
	b. cd <cyrus_sasl_root_folder>
	c. ./configure
	d. make
	e. [sudo] make install
	f. The the Cyrus SASL library depends on plugins which get installed into </usr/local/lib/sasl2> or
	</usr/lib/sasl2> folder by default. Create an environment variable called CYRUS_SASL_PLUGINS_DIR
	that points to that folder and export it, like below:
	`export CYRUS_SASL_PLUGINS_DIR=/usr/local/lib/sasl2`
	The driver depends on the above environment variable
10. [OPTIONAL if glog library is not installed]
	a. wget https://google-glog.googlecode.com/files/glog-0.3.3.tar.gz
	b. tar xzvf glog-0.3.3.tar.gz
	c. cd glog-0.3.3
	d. ./configure
	e. make
	f. [sudo] make install
	g. [sudo] ldconfig
11. To build the driver, do the following
	a. cd <hbase_root_folder>/hbase_native_client/src
	b. make all
	c. [sudo] make install
	Both the debug and release binaries are built
12. To run the tests, do the following
	a. cd <hbase_root_folder>/hbase_native_client/src/test
	b. chmod +x BuildTestClient.sh
	c. ./BuildTestClient.sh
	d. The above command produces a TestClient executable, both the debug and release ones.
	e. To run the TestClient, do a './TestClient <path_of_the_hbase_site.xml>' or
	 './TestClient_d <path_of_the_hbase_site.xml>'. '_d' means debug build.
