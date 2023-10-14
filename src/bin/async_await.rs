use std::{future::Future, time::Duration};

use futures::{stream::FuturesUnordered, StreamExt};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinSet,
};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    println!("Hello, world!");

    let mut join_set = JoinSet::new();

    let (fizz_handle, fut) = delayed_try_system(Duration::from_millis(10), 3);
    join_set.spawn(fut);

    let (buzz_handle, fut) = delayed_try_system(Duration::from_millis(10), 5);
    join_set.spawn(fut);

    let (fizzbuzz_handle, fut) = fizzbuzz_system(fizz_handle, buzz_handle);
    join_set.spawn(fut);

    (0..20)
        .map(|_| {
            let handle = fizzbuzz_handle.clone();
            async move { handle.request().await }
        })
        .collect::<FuturesUnordered<_>>()
        .map(|rep| {
            println!(
                "req: {}, fizz: {}, buzz: {}, fizz errors: {}, buzz errors: {}",
                rep.num, rep.fizz.num, rep.buzz.num, rep.fizz_errors, rep.buzz_errors
            )
        })
        .collect::<()>()
        .await;

    let stats = fizzbuzz_handle.stats().await;
    println!(
        "Total: {} requests, {} fizz errors, {} buzz errors",
        stats.request_conut, stats.fizz_errors, stats.buzz_errors
    );

    drop(fizzbuzz_handle);
    while let Some(result) = join_set.join_next().await {
        result.unwrap();
    }
}

// FizzBuzz

#[derive(Debug)]
enum FizzBuzzMessage {
    Reqest(FizzBuzzRequest),
    Stats(StatsRequest),
}

#[derive(Debug)]
struct FizzBuzzRequest {
    reply_to: oneshot::Sender<FizzBuzzResponse>,
}

#[derive(Debug)]
struct FizzBuzzResponse {
    num: usize,
    fizz: DelayedTrySuccess,
    buzz: DelayedTrySuccess,
    fizz_errors: usize,
    buzz_errors: usize,
}

#[derive(Debug)]
struct StatsRequest {
    reply_to: oneshot::Sender<StatsResponse>,
}

#[derive(Debug)]
struct StatsResponse {
    request_conut: usize,
    fizz_errors: usize,
    buzz_errors: usize,
}

#[derive(Clone)]
struct FizzBuzzHandle {
    inbox: mpsc::Sender<FizzBuzzMessage>,
}

impl FizzBuzzHandle {
    async fn request(&self) -> FizzBuzzResponse {
        let (tx, rx) = oneshot::channel();
        self.inbox
            .send(FizzBuzzMessage::Reqest(FizzBuzzRequest { reply_to: tx }))
            .await
            .expect("Can not send a request to FizzBuzz");
        rx.await.expect("Can not receive a respose from FizzBuzz")
    }

    async fn stats(&self) -> StatsResponse {
        let (tx, rx) = oneshot::channel();
        self.inbox
            .send(FizzBuzzMessage::Stats(StatsRequest { reply_to: tx }))
            .await
            .expect("Can not send a request to FizzBuzz");
        rx.await.expect("Can not receive a respose from FizzBuzz")
    }
}

enum ErrorEvent {
    Fizz,
    Buzz,
}

struct FizzBuzzSystem {
    error_tx: mpsc::Sender<ErrorEvent>,
    count: usize,
    fizz_errors: usize,
    buzz_errors: usize,
    fizz_handle: DelayedTryHandle,
    buzz_handle: DelayedTryHandle,
}

impl FizzBuzzSystem {
    fn on_request(&mut self, request: FizzBuzzRequest) -> impl 'static + Future<Output = ()> {
        self.count += 1;

        let num = self.count;
        let error_tx = self.error_tx.clone();
        let fizz_handle = self.fizz_handle.clone();
        let buzz_handle = self.buzz_handle.clone();

        let mut fizz_errors = 0;
        let mut buzz_errors = 0;

        async move {
            let fizz = loop {
                if let Ok(fizz) = fizz_handle.ask().await {
                    break fizz;
                }
                fizz_errors += 1;
                error_tx.send(ErrorEvent::Fizz).await.unwrap();
            };

            let buzz = loop {
                if let Ok(buzz) = buzz_handle.ask().await {
                    break buzz;
                }
                buzz_errors += 1;
                error_tx.send(ErrorEvent::Buzz).await.unwrap();
            };

            let response = FizzBuzzResponse {
                num,
                fizz,
                buzz,
                fizz_errors,
                buzz_errors,
            };
            request.reply_to.send(response).unwrap();
        }
    }

    fn on_stats(&self, request: StatsRequest) {
        let response = StatsResponse {
            request_conut: self.count,
            fizz_errors: self.fizz_errors,
            buzz_errors: self.buzz_errors,
        };
        request.reply_to.send(response).unwrap();
    }

    fn on_error(&mut self, evt: ErrorEvent) {
        match evt {
            ErrorEvent::Fizz => self.fizz_errors += 1,
            ErrorEvent::Buzz => self.buzz_errors += 1,
        }
    }
}

fn fizzbuzz_system(
    fizz_handle: DelayedTryHandle,
    buzz_handle: DelayedTryHandle,
) -> (FizzBuzzHandle, impl 'static + Future<Output = ()>) {
    let (main_tx, mut main_rx) = mpsc::channel::<FizzBuzzMessage>(16);
    let (error_tx, mut error_rx) = mpsc::channel::<ErrorEvent>(16);

    let mut system = FizzBuzzSystem {
        error_tx,
        count: 0,
        fizz_errors: 0,
        buzz_errors: 0,
        fizz_handle,
        buzz_handle,
    };

    let future = async move {
        let mut req_futures = FuturesUnordered::new();

        loop {
            tokio::select! {
                Some(message) = main_rx.recv() => {
                    match message {
                        FizzBuzzMessage::Reqest(request) => req_futures.push(system.on_request(request)),
                        FizzBuzzMessage::Stats(request) => system.on_stats(request),
                    }
                },
                Some(evt) = error_rx.recv(), if !req_futures.is_empty() => system.on_error(evt),
                Some(_) = req_futures.next() => (),
                else => break,
            };
        }
    };

    let handle = FizzBuzzHandle { inbox: main_tx };

    (handle, future)
}

// DelayedTry

#[derive(Debug)]
struct DelayedTryRequest {
    reply_to: oneshot::Sender<Result<DelayedTrySuccess, DelayedTryError>>,
}

#[derive(Debug)]
struct DelayedTrySuccess {
    num: usize,
}

#[derive(Debug, thiserror::Error)]
#[error("DelayedTry error")]
struct DelayedTryError;

#[derive(Clone)]
struct DelayedTryHandle {
    inbox: mpsc::Sender<DelayedTryRequest>,
}

impl DelayedTryHandle {
    async fn ask(&self) -> Result<DelayedTrySuccess, DelayedTryError> {
        let (tx, rx) = oneshot::channel();
        self.inbox
            .send(DelayedTryRequest { reply_to: tx })
            .await
            .expect("Can not send a request to DelayedTry");
        rx.await
            .expect("Can not receive a response from DelayedTry")
    }
}

fn delayed_try_system(
    delay: Duration,
    ok_every_nth: usize,
) -> (DelayedTryHandle, impl Future<Output = ()>) {
    let (tx, mut rx) = mpsc::channel::<DelayedTryRequest>(16);

    let future = async move {
        let mut num = 0;
        while let Some(req) = rx.recv().await {
            // Doing some operations that requires exclusive access to system's state
            num += 1;
            tokio::time::sleep(delay).await;
            if num % ok_every_nth == 0 {
                req.reply_to.send(Ok(DelayedTrySuccess { num }))
            } else {
                req.reply_to.send(Err(DelayedTryError))
            }
            .expect("Can not send response from DelayedTry")
        }
    };

    let handle = DelayedTryHandle { inbox: tx };

    (handle, future)
}
