Configuration
The erskafka plugin is configured through the ERS settings. Users that want make use of it need to define/extend the following ERS environment variables:

Tell ERS to load the erskafka plugin. The liberses.so shared library shall be in the LD_LIBRARY_PATH:

Se the partition name. The partition name allows to clearly distinguish the origin of the ERS messages, thus avoiding mixing information from different DAQ instances:
Extend the ERS variables which define the output streams to be used for Issues of different severities:

export DUNEDAQ_ERS_STREAM_LIBS=erskafka
export DUNEDAQ_PARTITION=ChooseYourPartitionName
export DUNEDAQ_ERS_INFO="erstrace,throttle(30,100),lstdout,erskafka(dunedaqutilities/erskafka)"
export DUNEDAQ_ERS_WARNING="erstrace,throttle(30,100),lstderr,erskafka(dunedaqutilities/erskafka)"
export DUNEDAQ_ERS_ERROR="erstrace,throttle(30,100),lstderr,erskafka(dunedaqutilities/erskafka)"
export DUNEDAQ_ERS_FATAL="erstrace,lstderr,erskafka(dunedaqutilities/erskafka)"

ERSKAFKA is not included in 2.6, if you are runnig 2.6, modify the build order to add erskafka directly after ERSES.
