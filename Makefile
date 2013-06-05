BIN := node_modules/.bin/
MOCHA := $(addprefix $(BIN), mocha)
TESTS := $(addprefix test/, test-*.js)

test:
	$(MOCHA) -R spec $(TESTS)

test-cov:
	$(MOCHA) --require blanket -R travis-cov $(TESTS)

coverage:
	$(MOCHA) --require blanket -R html-cov $(TESTS) > test/coverage.html

.PHONY: test test-cov coverage
