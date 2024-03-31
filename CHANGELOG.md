# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.7.0] - 2024-03-31
### Added
- feat: add TLS support ([#25](https://github.com/kezhuw/zookeeper-client-rust/pull/25)) ([#31](https://github.com/kezhuw/zookeeper-client-rust/pull/31))
- feat: seek quorum for readonly session ([#33](https://github.com/kezhuw/zookeeper-client-rust/pull/33))
- feat!: option to fail eagerly with `Error::NoHosts` ([#36](https://github.com/kezhuw/zookeeper-client-rust/pull/36))
- feat: add `Connector` to deprecate `ClientBuilder` ([#29](https://github.com/kezhuw/zookeeper-client-rust/pull/29))
- feat: add backoff between connection retries ([#37](https://github.com/kezhuw/zookeeper-client-rust/pull/37)) ([0e4e201](https://github.com/kezhuw/zookeeper-client-rust/commit/0e4e2018786bb5898585726bacac6dcc3ba33ea7))

### Changed
- refactor!: using tracing to carry context and log ([#40](https://github.com/kezhuw/zookeeper-client-rust/pull/40))
- refactor!: add `SessionInfo` to box session id, password and readonly ([58f8fb4](https://github.com/kezhuw/zookeeper-client-rust/commit/58f8fb4b6ade1d1d158dcebfd5b944ff5d534f76))
- chore: enrich logging message ([#41](https://github.com/kezhuw/zookeeper-client-rust/pull/41)) ([40c0a18](https://github.com/kezhuw/zookeeper-client-rust/commit/40c0a184b34df7e1b525cbd583c4e1af0fd8795b))
- chore: enrich tests ([309672d](https://github.com/kezhuw/zookeeper-client-rust/commit/309672d29c22879fe5d81f90a97bf01ec62efa8e)) ([373d21a](https://github.com/kezhuw/zookeeper-client-rust/commit/373d21a429d86e8e947b83359835d12a573da1ad))

### Fixed
- fix: `last_zxid_seen` in `ConnectRequest` is not set ([#39](https://github.com/kezhuw/zookeeper-client-rust/pull/39))

## [0.6.3] - 2024-03-12
### Fixed
- fix: session disconnected due to unblock multiple unwatching ([#24](https://github.com/kezhuw/zookeeper-client-rust/pull/24))
- fix: wrong error code for `Error::NoWatcher` ([#24](https://github.com/kezhuw/zookeeper-client-rust/pull/24))

### Changed
- chore: implement the `Debug` trait for `LockPrefix` and `LockPrefixInner` ([#21](https://github.com/kezhuw/zookeeper-client-rust/pull/21))

## [0.6.2] - 2024-01-17
### Changed
- fix: `Client::create` does not work on ZooKeeper 3.4.x ([#19](https://github.com/kezhuw/zookeeper-client-rust/pull/19))


## [0.6.1] - 2023-09-01
### Changed
- fix: crash due to use of freed node path ([d51bd54](https://github.com/kezhuw/zookeeper-client-rust/commit/d51bd54af4b99b0d5ed4d216f7d8a59a4281513d))

## [0.6.0] - 2023-09-01
### Added
- feat: add `mkdir` to create nodes recursively ([207c00f](https://github.com/kezhuw/zookeeper-client-rust/commit/207c00f7fe30b2899d32f277652ff2face45e0bc))
- feat: add `Display` for `ChrootPath` ([69f0d20](https://github.com/kezhuw/zookeeper-client-rust/commit/69f0d20d746e4e32920c9adcfb1c851d520c2c8b))
- feat!: carry `zxid` of transaction that triggering `WatchedEvent` ([b45d138](https://github.com/kezhuw/zookeeper-client-rust/commit/b45d138a1653ea0a762c0b3deb7841a584c4e43b))
- feat!: use `i64` for `CreateSequence` ([ab3017d](https://github.com/kezhuw/zookeeper-client-rust/commit/ab3017de612651a11c0d4f19bf45425cf589bf46))

### Changed
- fix!: forbid creation of root node "/" just like delete of it ([7e11a31](https://github.com/kezhuw/zookeeper-client-rust/commit/7e11a316eb65c5a5755abe1c46660393570c65db))
- fix: unwatching revived by failed watch could remove ongoing watching ([d20c161](https://github.com/kezhuw/zookeeper-client-rust/commit/d20c1614c44d6e8115f4f855e6fed9759c64ac0b))

[0.7.0]: https://github.com/kezhuw/zookeeper-client-rust/compare/v0.6.0...v0.7.0
[0.6.3]: https://github.com/kezhuw/zookeeper-client-rust/compare/v0.6.2...v0.6.3
[0.6.2]: https://github.com/kezhuw/zookeeper-client-rust/compare/v0.6.1...v0.6.2
[0.6.1]: https://github.com/kezhuw/zookeeper-client-rust/compare/v0.6.0...v0.6.1
[0.6.0]: https://github.com/kezhuw/zookeeper-client-rust/compare/v0.5.0...v0.6.0
