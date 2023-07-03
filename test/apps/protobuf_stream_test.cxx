
/* This application creates a ERSPublisher and dumps regular messages via the system
   
 */

#include <chrono>
#include <thread>
#include <sstream>

#include <ers/OutputStream.hpp>
#include <ers/StreamManager.hpp>

using namespace std;

ERS_DECLARE_ISSUE( erskafka,
                   TestIssue,
                   "this is issue with ID: " << id,
                   ((int)id)
                   )


int main( int argc, char * argv[] ) {

  std::string conf = "protobufstream(monkafka.cern.ch:30092)";

  setenv("DUNEDAQ_PARTITION", "TestPartition",0);
  setenv("DUNEDAQ_APPLICATION_NAME", "ERSStreamTest",0);
  
  auto stream = ers::StreamFactory::instance().create_out_stream( conf );
  
  int n = 20 ;
  for ( int i = 0 ; i < n ; ++i ) {
    erskafka::TestIssue issue(ERS_HERE, i);
    stream -> write( issue );
    this_thread::sleep_for(std::chrono::milliseconds(500+((i%2)*1000)));
  }
    
  return 0 ;
}
