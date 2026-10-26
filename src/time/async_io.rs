use std::time::Instant;

pub(crate) type Timer = async_io::Timer;

pub(crate) fn now() -> Instant {
    Instant::now()
}
