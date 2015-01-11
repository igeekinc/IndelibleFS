Indelible FS
============

Indelible FS is a distributed, scalable storage management system.  Indelible FS provides the following core functions:

* Traditional POSIX style file system
* Deduplication
* Versioning
* Replication
* Object storage
* Network APIs
* Scalability

Additional modules provide:

* Web access (IndelibleWebAccess)
* FUSE File system access (IndelibleFSFUSE)
* iSCSI virtual disk images (IndelibleVirtualSAN)

Building
--------
This is the core module.  OS specific modules are provided to build versions of Indelible FS that will run on that OS.
Currently supported operating systems are:

* Linux (IndelibleFS-Linux)
* Mac OS X (IndelibleFS-MacOSX)

Please refer to the README in those projects for instructions on how to build and install