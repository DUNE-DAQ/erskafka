/**
 * @file module.cpp. This performs python binding for the erskafka package.
 *
 * This is part of the DUNE DAQ Software Suite, copyright 2022.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "erskafka/KafkaStream.hpp"


namespace py = pybind11;

namespace erskafka {
namespace python {
	

PYBIND11_MODULE(_daq_erskafka_py, module) {

  py::class_<erskafka::KafkaStream, ers::OutputStream>(module, "KafkaStream")
    // constructor 
    .def(py::init<const std::string &>())
    // method
    .def("write", &erskafka::KafkaStream::write);
}

} // namespace python
} // namespace erskafka