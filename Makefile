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
	cargo test --features tls,fips-only,sasl,tokio,smol,async-global-executor
	cargo test --features turmoil --lib turmoil

doc:
	RUSTDOCFLAGS='--cfg docsrs' cargo +nightly doc --all-features
