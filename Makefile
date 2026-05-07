CHECKER_JAR ?= /home/soh/02_prog/xcsp3instances/XCSP3-Java-Tools/target/xcsp3-solutionChecker-2.6.0.jar
BENCHMARK_DIR ?= /home/soh/02_prog/benchmark
VALIDATION_WORKERS ?= 4

.PHONY: update-cop-results generate-cop-results validate-cop-results check-cop-results generate-cop-docs

update-cop-results:
	python3 scripts/update_cop_results.py --validate --checker-jar "$(CHECKER_JAR)" --benchmark-dir "$(BENCHMARK_DIR)" --validation-workers "$(VALIDATION_WORKERS)"
	python3 scripts/generate_cop_docs.py

generate-cop-results:
	python3 scripts/update_cop_results.py
	python3 scripts/generate_cop_docs.py

validate-cop-results:
	python3 scripts/update_cop_results.py --no-fetch --validate --checker-jar "$(CHECKER_JAR)" --benchmark-dir "$(BENCHMARK_DIR)" --validation-workers "$(VALIDATION_WORKERS)"
	python3 scripts/generate_cop_docs.py

generate-cop-docs:
	python3 scripts/generate_cop_docs.py

check-cop-results:
	python3 scripts/update_cop_results.py --no-fetch --validate --checker-jar "$(CHECKER_JAR)" --benchmark-dir "$(BENCHMARK_DIR)" --validation-workers "$(VALIDATION_WORKERS)"
	python3 scripts/generate_cop_docs.py
	git diff --exit-code -- cop-results.md docs/index.html logs
