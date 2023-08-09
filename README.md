# mariadbconnector
MariaDBConnector for FPC without driver

This is a small fpc project with a TMariaDBConnector for connecting to a MariaDB server without any driver.

Requirements
============

* FPC 2.6.4+ (others untested)
* Synapse 40+

How does it work?
=================

todo

Further Resources
=================
* https://mariadb.com/kb/en/clientserver-protocol/

Synapse requirement
=================
For communication Synapse from Ararat is used. You can download the latest version from https://sourceforge.net/p/synalist/code/HEAD/tree/trunk/
(at the top-right is a "Download snapshot"-button. Put it somewhere and in Lazarus you can choose "Package" > "Open package file".
Browse to the folder for synapse and choose laz_synapse.lpk.
Extra step is to add the ssl_openssl.pas before compiling for HTTPS access.
You don't need to install anything. laz_synapse will now be available as package.
You also need the openssl DLLs in your project directory (or search-path). libeay32.dll, libssl32.dll and ssleay32.dll.

Todo
====

* Improve the documentation and comments
* Making BINARY Protocol functions
* Making FetchRecords for BINARY Protocol

