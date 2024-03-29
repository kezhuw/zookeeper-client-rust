use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::time::{self, Instant, Sleep};

pub struct Deadline {
    sleep: Option<Sleep>,
}

impl Deadline {
    pub fn never() -> Self {
        Self { sleep: None }
    }

    pub fn until(deadline: Instant) -> Self {
        Self { sleep: Some(time::sleep_until(deadline)) }
    }

    pub fn elapsed(&self) -> bool {
        self.sleep.as_ref().map(|f| f.is_elapsed()).unwrap_or(false)
    }
}

impl Future for Deadline {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.sleep.is_none() {
            return Poll::Pending;
        }
        let sleep = unsafe { self.map_unchecked_mut(|deadline| deadline.sleep.as_mut().unwrap_unchecked()) };
        sleep.poll(cx)
    }
}
