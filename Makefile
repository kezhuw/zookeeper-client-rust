check: check_fmt lint doc

verify: check build test

fmt:
	cargo +nightly fmt --all

check_fmt:
	cargo +nightly fmt --all -- --check

lint:
	cargo clippy --all-targets --all-features --no-deps -- -D clippy::all

build:
	RUSTFLAGS=-Dunused-crate-dependencies cargo build-all-features

test:
	cargo test --all-features

doc:
	RUSTDOCFLAGS='--cfg docsrs' cargo +nightly doc --all-features
