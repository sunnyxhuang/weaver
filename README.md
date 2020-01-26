# Weaver : Efficient Coflow Scheduling in Hetrogeous Parallel Networks
 
Weaver is an algorithm to efficiently schedule Coflows in the Hetrogeous 
Parallel Networks (HPNs). HPNs is a network architecture that supports
incremental network evolutions. Our Coflow scheduling objective is to optimize
the Coflow performance (e.g. Coflow Completion Time, or CCT) to speed up 
the application. This work is described in:

>[Weaver: Efficient Coflow Scheduling in Heterogeneous Parallel Networks](https://www.cs.rice.edu/~eugeneng/papers/IPDPS20.pdf)

>[Xin Sunny Huang](http://www.cs.rice.edu/~xinh/), 
[Yiting Xia](http://yitingxia.rice.edu),
and [T. S. Eugene Ng](http://www.cs.rice.edu/~eugeneng/)  

> In _34th IEEE International Parallel & Distributed Processing Symposium ([IPDPS 2020](http://www.ipdps.org/))_

This repository contains a flow-level, discrete-event simulator, 
along with the tools necessary to replicate the experimental results reported 
in the paper. 

## Prerequisite ##

### [Gurobi](http://www.gurobi.com), the LP solver. ### 
A Gurobi license should be installed for our simulator to run. 
See [link](http://www.gurobi.com/academia/for-universities) for additional
information on how to install the Gurobi license if you are a university user.

Depending on whether you are running on the Mac or Linux, make sure the 
following libraries or header are under the directory of `/gurobi/mac64/` or
`/gurobi/linux64/`.

* `gurobi_c.h`
* `gurobi_c++.h`
* `libgurobi70.so` (exact file name depends on the version of Gurobi)
* `libgurobi_c++.a` (or `libgurobi_g++4.2.a`)

### [CMake](https://cmake.org)

By default, our simulator requires CMake to build the
binary. You may also write your own Makefile to bypass CMake.

## How to run ##
The minimal working set is in `src/`, with a `CMakeList` to build the simulator 
binary. You can start the simulator with 

``` 
mkdir build
cd build/
cmake ../src/
make
./Ximulator [options]
```
 
Take a look at `main.cc` for the flag options that you may want to reconfigure. 
For example, to play with a specific scheduler, use

``` 
./Ximulator -s scheduler_name 
```

## Output / Logging ##
The simulator is originally designed to dump all performance metrics (e.g. CCTs) 
 into an external database for data analysis. 
 However, database connection requires installing new library and driver. 
 To remove such dependency and allow quick compilation on any machine, I remove or comment out code that relies on database connection. 
 Instead of writing metrics to database, you may also write out to a file. Please refer to to `db_logger.{h, cc}` for more details. 

## IDE ##
This whole package works with CLion, so it comes with lots of `CMakeLists.txt`. 
To use IDE which may have a different directory for the binary other than 
`src/`, you may want to reconfigure the `MAC_BASE_DIR` in `global.cc`, 
to help the binary to find out the correct directory for input and output files.
For details, take a look at `global.cc`.

I have provided simple tests for different components under 
`tests/test_src/`. You might want to begin with the tests for 
all schedulers in `ximulator_test.cc`. 
Our tests requires [GoogleTest](https://github.com/google/googletest.git) as a submodule.
Run the following command to download the GoogleTest library to compile the test binaries.
 ``` 
 cd tests/test_lib
 git submodule init
 git submodule update 
 ``` 

Then you may compile the tests as well as the simulator binaries using the CMakeLists.txt in the root directory. 
 ``` 
 mkdir build
 cd build/
 cmake ../
 make
 ```
Try to find the test binaries under subdirectory `build/tests`.
To run the test, you may also need to reconfigure `MAC_BASE_DIR` in `ximulator_test_base.h`, so as to help the IDE to find the tests and the necessary input or output files. 
Due to randomization in the simulation, the tests may fail when running in a different environment.  

## Trace ##
The Coflow trace comes from the popular
[coflow-benchmark](https://github.com/coflow/coflow-benchmark) by 
[Mosharaf Chowdhury](https://github.com/mosharaf), with minor reformatting.
   
## History ##
This simulator is developed over our previous open-sourced 
[simulator #1](https://github.com/sunnyxhuang/sunflow) 
and [simulator #2](https://github.com/sunnyxhuang/2d-placement)
for our [Sunflow](http://www.cs.rice.edu/%7Eeugeneng/papers/CoNEXT16.pdf)
and [2D-Placement](http://www.cs.rice.edu/%7Eeugeneng/papers/APNET17.pdf) 
projects.
However, each simulator is designed to run independently on its own.
