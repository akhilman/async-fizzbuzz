use std::{time::Duration, sync::Arc};

use actix::{Actor, Addr, AsyncContext, Context, Handler, Message, MessageResult, ResponseFuture};
use futures::{stream::FuturesUnordered, StreamExt};
use tokio::sync::Mutex;

#[actix::main]
async fn main() {
    println!("Hello, world!");

    let fizz_addr = DelayesTryService::new(Duration::from_millis(10), 3).start();
    let buzz_addr = DelayesTryService::new(Duration::from_millis(10), 5).start();
    let fizzbuzz_addr = FizzBuzzService::new(fizz_addr, buzz_addr).start();

    (0..20)
        .map(|_| {
            let addr = fizzbuzz_addr.clone();
            async move { addr.send(FizzBuzzRequest).await.unwrap() }
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

    let stats = fizzbuzz_addr.send(StatsRequest).await.unwrap();
    println!(
        "Total: {} requests, {} fizz errors, {} buzz errors",
        stats.request_conut, stats.fizz_errors, stats.buzz_errors
    );

    drop(fizzbuzz_addr);
}

// FizzBuzz

#[derive(Debug, Message)]
#[rtype(result = "FizzBuzzResponse")]
struct FizzBuzzRequest;

#[derive(Debug)]
struct FizzBuzzResponse {
    num: usize,
    fizz: DelayedTrySuccess,
    buzz: DelayedTrySuccess,
    fizz_errors: usize,
    buzz_errors: usize,
}

#[derive(Debug, Message)]
#[rtype(result = "StatsResponse")]
struct StatsRequest;

struct StatsResponse {
    request_conut: usize,
    fizz_errors: usize,
    buzz_errors: usize,
}

#[derive(Debug, Message)]
#[rtype(result = "()")]
struct FizzFaultNotification;

#[derive(Debug, Message)]
#[rtype(result = "()")]
struct BuzzFaultNotification;

struct FizzBuzzService {
    count: usize,
    fizz_errors: usize,
    buzz_errors: usize,
    fizz_addr: Addr<DelayesTryService>,
    buzz_addr: Addr<DelayesTryService>,
}

impl FizzBuzzService {
    fn new(fizz_addr: Addr<DelayesTryService>, buzz_addr: Addr<DelayesTryService>) -> Self {
        Self {
            count: 0,
            fizz_errors: 0,
            buzz_errors: 0,
            fizz_addr,
            buzz_addr,
        }
    }
}

impl Actor for FizzBuzzService {
    type Context = Context<Self>;
}

impl Handler<FizzBuzzRequest> for FizzBuzzService {
    type Result = ResponseFuture<FizzBuzzResponse>;

    fn handle(&mut self, _msg: FizzBuzzRequest, ctx: &mut Self::Context) -> Self::Result {
        self.count += 1;

        let num = self.count;
        let fizz_addr = self.fizz_addr.clone();
        let buzz_addr = self.buzz_addr.clone();
        let self_addr = ctx.address();

        let mut fizz_errors = 0;
        let mut buzz_errors = 0;

        Box::pin(async move {
            let fizz = loop {
                if let Ok(fizz) = fizz_addr.send(DelayedTryRequest).await.unwrap() {
                    break fizz;
                }
                fizz_errors += 1;
                self_addr.send(FizzFaultNotification).await.unwrap();
            };
            let buzz = loop {
                if let Ok(buzz) = buzz_addr.send(DelayedTryRequest).await.unwrap() {
                    break buzz;
                }
                buzz_errors += 1;
                self_addr.send(BuzzFaultNotification).await.unwrap();
            };
            FizzBuzzResponse {
                num,
                fizz,
                buzz,
                fizz_errors,
                buzz_errors,
            }
        })
    }
}

impl Handler<StatsRequest> for FizzBuzzService {
    type Result = MessageResult<StatsRequest>;

    fn handle(&mut self, _msg: StatsRequest, _ctx: &mut Self::Context) -> Self::Result {
        MessageResult(StatsResponse {
            request_conut: self.count,
            fizz_errors: self.fizz_errors,
            buzz_errors: self.buzz_errors,
        })
    }
}

impl Handler<FizzFaultNotification> for FizzBuzzService {
    type Result = ();

    fn handle(&mut self, _msg: FizzFaultNotification, _ctx: &mut Self::Context) -> Self::Result {
        self.fizz_errors += 1;
    }
}
impl Handler<BuzzFaultNotification> for FizzBuzzService {
    type Result = ();

    fn handle(&mut self, _msg: BuzzFaultNotification, _ctx: &mut Self::Context) -> Self::Result {
        self.buzz_errors += 1;
    }
}

// DelayedTry

#[derive(Debug, Message)]
#[rtype(result = "DelayedTryResponse")]
struct DelayedTryRequest;

type DelayedTryResponse = Result<DelayedTrySuccess, DelayedTryError>;

#[derive(Debug)]
struct DelayedTrySuccess {
    num: usize,
}

#[derive(Debug, thiserror::Error)]
#[error("DelayedTry error")]
struct DelayedTryError;

struct DelayesTryService {
    delay: Duration,
    ok_every_nth: usize,
    count: Arc<Mutex<usize>>,
}

impl DelayesTryService {
    fn new(delay: Duration, ok_every_nth: usize) -> Self {
        Self {
            delay,
            ok_every_nth,
            count: Arc::new(Mutex::new(0)),
        }
    }
}

impl Actor for DelayesTryService {
    type Context = Context<Self>;
}

impl Handler<DelayedTryRequest> for DelayesTryService {
    type Result = ResponseFuture<DelayedTryResponse>;

    fn handle(&mut self, _msg: DelayedTryRequest, _ctx: &mut Self::Context) -> Self::Result {
        let ok_every_nth = self.ok_every_nth;
        let delay = self.delay;
        let count = self.count.clone();

        Box::pin(async move {
            let mut count = count.lock().await;
            // Doing some operations that requires exclusive access to service's state
            *count += 1;
            tokio::time::sleep(delay).await;
            if *count % ok_every_nth == 0 {
                Ok(DelayedTrySuccess { num: *count })
            } else {
                Err(DelayedTryError)
            }
        })
    }
}
