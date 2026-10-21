#define CATCH_CONFIG_RUNNER

#include "catch/catch.hpp"
#include <cstdio>
#include <cstdlib>
#include <exception>

int main(int argc, char *argv[]) {
	try {
		return Catch::Session().run(argc, argv);
	} catch (const std::exception &error) {
		std::fprintf(stderr, "Test runner failed: %s\n", error.what());
	} catch (...) {
		std::fprintf(stderr, "Test runner failed with an unknown exception\n");
	}
	return EXIT_FAILURE;
}
