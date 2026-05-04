.PHONY: update-cop-results check-cop-results

update-cop-results:
	python3 scripts/update_cop_results.py

check-cop-results:
	python3 scripts/update_cop_results.py --no-fetch
	git diff --exit-code -- cop-results.md logs
