# Implementation and Performance Tuning of a Scalable Web Service
**[15-440/640 Distributed Systems](https://www.andrew.cmu.edu/course/15-440/) at Carnegie Mellon University**
***
<br>

This provides the classes needed for Project 3.

The main classes (Cloud, ServerLib, Database, ClientSim) and the sample
database file (db1.txt) are in the lib directory.  

A sample server is provided in the sample directory.  To build this, ensure
your CLASSPATH has the lib and sample directory included.  Then run make in 
the sample directory.  

To run the sample, try:
	java Cloud 15440 lib/db1.txt c-2000-1 12
This will launch the "Cloud" service, load the database with the items in 
lib/db1.txt, and start simulating clients arriving at a constant rate every
2000 ms.  A single instance of the sample Server will b erun as a "VM" 
(actually a spearate process).  

See the handout pdf for more details.