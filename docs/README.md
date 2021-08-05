## Configuration
The erskafka plugin is configured through the [ERS](https://dune-daq-sw.readthedocs.io/en/latest/packages/ers/) settings. Users that want make use of it need to [set up a work area environment](https://dune-daq-sw.readthedocs.io/en/latest/packages/daq-buildtools/) and define/extend the following ERS environment variables as described on this page. 

Tell ERS to load the erskafka plugin:
```
export DUNEDAQ_ERS_STREAM_LIBS=erskafka
```

Set the partition name. The partition name allows to clearly distinguish the origin of the ERS messages, thus avoiding mixing information from different DAQ instances:
```
export DUNEDAQ_PARTITION=ChooseYourPartitionName
```

Extend the ERS variables which define the output streams to be used for Issues of different severities:
```
export DUNEDAQ_ERS_INFO="erstrace,throttle(30,100),lstdout,erskafka(dqmbroadcast:9092)"
export DUNEDAQ_ERS_WARNING="erstrace,throttle(30,100),lstderr,erskafka(dqmbroadcast:9092)"
export DUNEDAQ_ERS_ERROR="erstrace,throttle(30,100),lstderr,erskafka(dqmbroadcast:9092)"
export DUNEDAQ_ERS_FATAL="erstrace,lstderr,erskafka(dqmbroadcast:9092)"
```

erskafka is not included in the `dunedaq-2.6.0` frozen release, if you are running `dunedaq-v2.6.0`, modify the build order to add erskafka directly after [erses](https://dune-daq-sw.readthedocs.io/en/latest/packages/erses/) (by default `YourWorkarea/sourcecode/dbt-build-order.cmake`). Do check that you have access to the kafka library from dune externals (`librdkafka        v1_7_0       e19:prof`) from `/cvmfs/dunedaq-development.opensciencegrid.org/products`.

Default platform: https://dunedaqreporting.app.cern.ch/ErrorReports
