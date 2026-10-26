use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

/// Reads tokio's clock, which turmoil advances, as a `std` instant so the client's `Instant`
/// arithmetic is unchanged.
pub(crate) fn now() -> Instant {
    tokio::time::Instant::now().into_std()
}

/// `async_io::Timer` on tokio's clock: a one-shot or periodic timer whose output is the instant it
/// was scheduled to fire.
#[derive(Debug)]
pub(crate) struct Timer(Kind);

#[derive(Debug)]
enum Kind {
    Never,
    // Boxed because tokio's `Sleep` must not move once polled.
    Once(Pin<Box<tokio::time::Sleep>>),
    Periodic(tokio::time::Interval),
}

impl Timer {
    pub(crate) fn after(duration: Duration) -> Self {
        Self(Kind::Once(Box::pin(tokio::time::sleep(duration))))
    }

    pub(crate) fn at(deadline: Instant) -> Self {
        Self(Kind::Once(Box::pin(tokio::time::sleep_until(deadline.into()))))
    }

    /// Like `async_io::Timer::interval`, the first tick is one `period` out, while tokio's
    /// `interval` ticks at once.
    pub(crate) fn interval(period: Duration) -> Self {
        match tokio::time::Instant::now().checked_add(period) {
            Some(start) => Self(Kind::Periodic(tokio::time::interval_at(start, period))),
            None => Self::never(),
        }
    }

    pub(crate) fn never() -> Self {
        Self(Kind::Never)
    }
}

impl Future for Timer {
    type Output = Instant;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Instant> {
        match &mut self.get_mut().0 {
            Kind::Never => Poll::Pending,
            Kind::Once(sleep) => sleep.as_mut().poll(cx).map(|()| sleep.deadline().into_std()),
            Kind::Periodic(interval) => interval.poll_tick(cx).map(tokio::time::Instant::into_std),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{now, Timer};

    #[test]
    fn timers_fire_on_simulated_time() -> turmoil::Result {
        let mut sim = turmoil::Builder::new().simulation_duration(Duration::from_secs(60)).build();

        sim.client("client", async {
            let second = Duration::from_secs(1);
            let start = now();
            assert_eq!(Timer::after(second).await, start + second);
            assert_eq!(Timer::at(start + 3 * second).await, start + 3 * second);

            let start = now();
            let mut interval = Timer::interval(2 * second);
            assert_eq!((&mut interval).await, start + 2 * second);
            assert_eq!((&mut interval).await, start + 4 * second);
            assert!(now() >= start + 4 * second);
            Ok(())
        });

        sim.run()
    }
}
