.PHONY: install update format lint test clean clean-appledouble

# The checkout may live on an exFAT volume which emits macOS AppleDouble
# sidecars. `._foo_spec.cr` matches crystal's `*_spec.cr` glob, so strip them
# before any tool that globs the tree.
clean-appledouble:
	@find . -name '._*' -not -path './vendor/*' -not -path './.git/*' -delete 2>/dev/null || true

install:
	shards install

update:
	shards update

format: clean-appledouble
	crystal tool format src spec

lint: clean-appledouble
	crystal tool format --check src spec
	ameba src spec

test: clean-appledouble
	crystal spec

clean:
	rm -rf ./temp/*
