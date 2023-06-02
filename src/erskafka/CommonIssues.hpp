/**
 * @file CommonIssues.hpp
 *
 * This file contains the definitions of ERS Issues that are common
 * among the ERS streamer classes
 *
 * This is part of the DUNE DAQ Software Suite, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#ifndef ERSKAFKA_SRC_ERSKAFKA_COMMONISSUES_HPP_
#define ERSKAFKA_SRC_ERSKAFKA_COMMONISSUES_HPP_

#include "ers/Issue.hpp"
//#include "appfwk/DAQModule.hpp"

#include <string>

namespace dunedaq {

// Disable coverage checking LCOV_EXCL_START

    ERS_DECLARE_ISSUE(erskafka,
                    MissingInfo,
                    "JSON Missing " << json_entry,
                    ((std::string)json_entry)
                    )


    ERS_DECLARE_ISSUE(erskafka,
		      FailedConfig,
		      json_entry << " failed to configure: " << message,
		      ((std::string)json_entry)((std::string)message)
		      )
  
    
// ERS_DECLARE_ISSUE(erskafka,                                                                                        
//                FailingCreatingStreamer,                                                                            
//                "ERS Streamer creation failed",                                                                     
//                ERS_EMPTY)                                                                                          

// ERS_DECLARE_ISSUE(erskafka,                                                                                        
//                ProductionFailed,                                                                                   
//                "Failed to produce: " << error,                                                                     
//                ((std::string)error))                                                                               

// ERS_DECLARE_ISSUE_BASE(erskafka,                                                                                   
//                     ProductionFailedOnTopic,                                                                       
//                     ProductionFailed,                                                                              
//                     "Failed to produced to topic " << topic << ": " << error,                                      
//                     ((std::string)error),                                                                          
//                     ((std::string)topic))                                                                          


  
} // namespace dunedaq

#endif // ERSKAFKA_SRC_ERSKAFKA_COMMONISSUES_HPP_
