#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <iostream>
#include <vector>
#include <string>
#include <cstdlib>

namespace py = pybind11;

std::string execute_substrait_query(const std::string& query_plan_path, const std::vector<std::string>& parquet_files) {
    std::string velox_executable = "/opt/velox/bin/velox_substrait";

    // Construct the command with multiple Parquet files
    std::string command = velox_executable + " --plan " + query_plan_path + " --files";
    for (const auto& file : parquet_files) {
        command += " " + file;
    }

    std::cout << "Executing command: " << command << std::endl;

    // Run the command
    FILE* pipe = popen(command.c_str(), "r");
    if (!pipe) {
        throw std::runtime_error("Failed to run Velox command");
    }

    // Capture the output
    char buffer[128];
    std::string result = "";
    while (fgets(buffer, sizeof(buffer), pipe) != nullptr) {
        result += buffer;
    }

    pclose(pipe);
    return result;
}

// Expose to Python
PYBIND11_MODULE(velox_wrapper, m) {
    m.def("execute_substrait_query", &execute_substrait_query, "Execute Substrait query with Velox",
          py::arg("query_plan_path"), py::arg("parquet_files"));
}