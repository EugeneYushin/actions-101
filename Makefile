.PHONY: *

fill-variables:
	envsubst < composer/_variables_REPORTS.json > composer/variables_REPORTS.json

test: fill-variables
	jq -s '{"REPORTS": .[0]}' composer/variables_REPORTS.json > composer/variables.json
	pytest -p no:warnings .