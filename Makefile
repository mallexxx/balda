.PHONY: dev scenarios

dev:
	@./scripts/dev/run-balda-embedded-jetstream.sh

scenarios:
	@./scripts/dev/run-fake-ingress-scenarios.sh
