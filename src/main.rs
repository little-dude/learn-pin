use std::{
    collections::VecDeque,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use futures::{
    stream::{FuturesUnordered, Stream, StreamExt},
    Future,
};
use tokio::{
    task::{JoinError, JoinHandle},
    time::delay_for,
};

/// `ConcurrentFutures` can keep a capped number of futures running concurrently, and yield their
/// result as they finish. When the max number of concurrent futures is reached, new tasks are
/// queued until some in-flight futures finish.
pub struct ConcurrentFutures<T>
where
    T: Future + Send + 'static,
    T::Output: Send + 'static,
{
    /// in-flight futures
    running: FuturesUnordered<JoinHandle<T::Output>>,
    /// buffered tasks
    pending: VecDeque<T>,
    /// max number of concurrent futures
    max_in_flight: usize,
}

impl<T> ConcurrentFutures<T>
where
    T: Future + Send + 'static,
    T::Output: Send + 'static,
{
    pub fn new(max_in_flight: usize) -> Self {
        Self {
            running: FuturesUnordered::new(),
            pending: VecDeque::new(),
            max_in_flight,
        }
    }

    pub fn push(&mut self, task: T) {
        self.pending.push_back(task)
    }

    fn project_running(self: Pin<&mut Self>) -> Pin<&mut FuturesUnordered<JoinHandle<T::Output>>> {
        // This is okay because:
        // - the closure doesn't move out for `s`
        // - the returned value cannot be moved as long as `s` is not moved
        // See: https://doc.rust-lang.org/std/pin/struct.Pin.html#method.map_unchecked_mut
        unsafe { self.map_unchecked_mut(|s| &mut s.running) }
    }

    fn project_pending(self: Pin<&mut Self>) -> &mut VecDeque<T> {
        // Not sure whether this is actually safe. When `ConcurrentFutures` is pinned, should we
        // be able to push/pop from one of the fields?
        unsafe { &mut self.get_unchecked_mut().pending }
    }
}

impl<T> Stream for ConcurrentFutures<T>
where
    T: Future + Send + 'static,
    T::Output: Send + 'static,
{
    type Item = Result<T::Output, JoinError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        while self.running.len() < self.max_in_flight {
            if let Some(pending) = self.as_mut().project_pending().pop_front() {
                let handle = tokio::spawn(pending);
                self.running.push(handle);
            } else {
                break;
            }
        }
        self.as_mut().project_running().poll_next(cx)
    }
}

#[tokio::main]
async fn main() {
    let mut stream =
        ConcurrentFutures::<Pin<Box<dyn Future<Output = u8> + Send + 'static>>>::new(2);

    stream.push(Box::pin(async {
        delay_for(Duration::from_millis(1000_u64)).await;
        1_u8
    }));

    stream.push(Box::pin(async {
        delay_for(Duration::from_millis(2500_u64)).await;
        2_u8
    }));

    stream.push(Box::pin(async {
        delay_for(Duration::from_millis(1200_u64)).await;
        3_u8
    }));

    stream.push(Box::pin(async {
        delay_for(Duration::from_millis(1_u64)).await;
        4_u8
    }));

    // poll_next hasn't been called yet so nothing is running
    assert_eq!(stream.running.len(), 0);
    assert_eq!(stream.pending.len(), 4);
    assert_eq!(stream.next().await.unwrap().unwrap(), 1);

    // two futures have been spawned, but one of them just finished: one is still running, two are
    // still pending
    assert_eq!(stream.running.len(), 1);
    assert_eq!(stream.pending.len(), 2);
    assert_eq!(stream.next().await.unwrap().unwrap(), 3);

    // three futures have been spawned, two finished: one is still running, one is still pending
    assert_eq!(stream.running.len(), 1);
    assert_eq!(stream.pending.len(), 1);
    assert_eq!(stream.next().await.unwrap().unwrap(), 4);

    // four futures have been spawn, three finished: one is still running
    assert_eq!(stream.next().await.unwrap().unwrap(), 2);
    assert_eq!(stream.running.len(), 0);
    assert_eq!(stream.pending.len(), 0);
}
