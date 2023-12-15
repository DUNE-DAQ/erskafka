
/* This application creates a ERSPublisher and dumps regular messages via the system
   
 */

#include <chrono>
#include <thread>
#include <sstream>

#include <ers/ers.hpp>
#include <erskafka/ERSPublisher.hpp>

using namespace std;
using namespace dunedaq::erskafka;

ERS_DECLARE_ISSUE( erstest,
		   TestIssue,
		   "this is issue with ID: " << id,
		   ((int)id)
		   )

int main( int argc, char * argv[] ) {

  nlohmann::json conf;
  conf["bootstrap"] = "monkafka.cern.ch:30092";

  ERSPublisher p(conf);

  int n = 20 ;
  for ( int i = 0 ; i < n ; ++i ) {
    erstest::TestIssue issue(ERS_HERE, i);
    p.publish( to_schema_chain(issue) );
    this_thread::sleep_for(std::chrono::milliseconds(500));
  }
  
  
  return 0 ;
}
