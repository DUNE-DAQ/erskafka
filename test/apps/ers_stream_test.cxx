
/* This application creates a ERSPublisher and dumps regular messages via the system
   
 */

#include <chrono>
#include <thread>
#include <sstream>

#include <erskafka/ERSPublisher.hpp>

using namespace std;
using namespace dunedaq::erskafka;


int main( int argc, char * argv[] ) {

  // nlohmann::json conf;
  // conf["bootstrap"] = "monkafka.cern.ch:30092";
  // conf["partition"] = "ers_stream_test";

  // ERSPublisher p(conf);

  // int n = 20 ;
  // for ( int i = 0 ; i < n ; ++i ) {
  //   dunedaq::ersschema::IssueChain c;
  //   stringstream ss;
  //   ss << "Message number " << i ;
  //   (*c.mutable_message()).set_message(ss.str());
  //   p.publish( std::move(c) );
  //   this_thread::sleep_for(std::chrono::milliseconds(500));
  // }
  
  
  return 0 ;
}
