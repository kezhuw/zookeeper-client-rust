use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use async_io::Timer;
use futures::future::{Fuse, FusedFuture, FutureExt};

pub struct Deadline {
    timer: Fuse<Timer>,
    deadline: Option<Instant>,
}

impl Deadline {
    pub fn never() -> Self {
        Self { timer: Timer::never().fuse(), deadline: None }
    }

    pub fn until(deadline: Instant) -> Self {
        Self { timer: Timer::at(deadline).fuse(), deadline: Some(deadline) }
    }

    pub fn elapsed(&self) -> bool {
        self.timer.is_terminated()
    }

    /// Remaining timeout.
    pub fn timeout(&self) -> Duration {
        match self.deadline.as_ref() {
            None => Duration::MAX,
            Some(deadline) => deadline.saturating_duration_since(Instant::now()),
        }
    }
}

impl Future for Deadline {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let timer = unsafe { self.map_unchecked_mut(|deadline| &mut deadline.timer) };
        timer.poll(cx).map(|_| ())
    }
}
