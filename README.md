Lagerta
==========================================

Lagerta is an open source transactional data transport. It supports light snapshots creation 
and management for in-memory data grids and fabrics.

Features
==========================================

Quick overview of source code directories
=========================================


Contributing
============

We are always open to people who want to use the system or contribute to it. 
People with different expertise can help with various subprojects:
    
* __core__ :    Core functionality of the replicator, where most of the value of the project sits
* __cassandra__ : Reference implementation of the long-term storage layer
* __doc__ : We are always in need of a better documentation!
* __lagerta-core__ : core module 
* __lagerta-test__ : environments and resources to test your project integrated with Lagerta
* __lagerta-jepsen__ : tests HA and DR
* __lagerta-jmh__ : tests performance

* __giving feedback__ : Tell us how you use Lagerta, what was great and what was not so great.
                        Also, what are you expecting from it and what would you like to see in the future?
 
Also, reporting new issues and getting started by posting on the mailing list is helpful.

RTC model
=========

Lagerta is developed under Review-Then-Commit (RTC) model. The following rules
will be used for the RTC process:
  * a committer or a contributor should proactively seek a code review and
    feedback from other contributors. To speed up the review process the common
    sense quality criteria are expected to be met (e.g. reasonable testing has
    been done locally; all compilations pass; RAT check is passed; the patch
    follows coding guidelines)
  * a committer should keep an eye on the official CI builds at
    Travis CI to make sure that committed changes haven't break anything. In
    which case the committer should take a timely effort to resolve the
    issues and unblock the others in the community
  * any non-document patch needs to address all the comment and reach consensus
    before it gets committed without a +1 from other committers

What do people use Lagerta for?
==================================


Getting Started
===============
If you want add Lagerta as a dependency to you project use [jitpack](https://jitpack.io)

For Developers: Running the tests.
----------------------------------
Use maven for testing
```$bash
mvn -pl '!core,!cassandra' clean verify
```

For Developers: Building the project
------------------------------------
Use maven for build the project.
```$bash
mvn -pl '!core,!cassandra' clean install -DskipTests
```

For Developers: Building a component from Git repository
--------------------------------------------------------
```$bash
git clone https://github.com/epam/Lagerta
cd ./Lagerta
mvn -pl '!core,!cassandra' install
```

Contact us
----------

You can get in touch with us on [gitter](https://gitter.im/lagerta-room/Lobby#)

## Documentation

The documentation of Lagerta is located on the [github wiki](https://github.com/epam/Lagerta/wiki) 
and in the `doc/` directory of the source code.

## About

Lagerta is an open source project under The Apache License 2.0