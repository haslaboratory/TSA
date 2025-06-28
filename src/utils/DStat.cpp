#include <fstream>

#include "utils/DStat.h"

void dump_vector(std::string name, std::vector<double> *v) {
    std::ofstream out(name);
    for (auto &x : *v) {
        out << x << std::endl;
    }
}

void load_vector(std::string name, std::vector<double> *v) {
    std::ifstream in(name);
    double x;
    while (in >> x) {
        v->push_back(x);
    }
}