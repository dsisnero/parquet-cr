.PHONY: install update format lint test test-extensions clean clean-appledouble

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
	crystal tool format src spec bin examples bench

lint: clean-appledouble
	crystal tool format --check src spec bin examples bench
	ameba src spec bin examples bench

test: clean-appledouble
	crystal spec

# Nondefault upstream Cargo features are matching Crystal compile flags.
test-extensions: clean-appledouble
	crystal spec -Darrow_canonical_extension_types -Dgeospatial -Dvariant_experimental

clean:
	rm -rf ./temp/*
