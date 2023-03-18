use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use std::task::{Poll, Waker};
use std::time::Duration;

use futures::future::BoxFuture;
use futures::task::{waker_ref, ArcWake};
use futures::Future;

use rand::prelude::*;
use std::thread;

pub struct TimerFuture {
    shared_state: Arc<Mutex<SharedState>>,
}

struct SharedState {
    completed: bool,
    waker: Option<Waker>,
}

impl Future for TimerFuture {
    type Output = ();
    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let mut shared_state = self.shared_state.lock().unwrap();

        if shared_state.completed {
            Poll::Ready(())
        } else {
            shared_state.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

impl TimerFuture {
    pub fn new(duration: Duration) -> Self {
        let shared_state = Arc::new(Mutex::new(SharedState {
            completed: false,
            waker: None,
        }));

        let thread_shared_state = shared_state.clone();

        thread::spawn(move || {
            thread::sleep(duration);
            let mut shared_state = thread_shared_state.lock().unwrap();

            shared_state.completed = true;

            if let Some(waker) = shared_state.waker.take() {
                waker.wake()
            }
        });

        TimerFuture { shared_state }
    }
}

struct Task {
    future: Mutex<Option<BoxFuture<'static, ()>>>,
    task_sender: SyncSender<Arc<Task>>,
}

impl ArcWake for Task {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        let cloned = arc_self.clone();
        arc_self.task_sender.send(cloned).expect("Too many tasks");
    }
}

struct Executor {
    ready_queue: Receiver<Arc<Task>>,
}

impl Executor {
    fn run(&self) {
        while let Ok(task) = self.ready_queue.recv() {
            let mut future_slot = task.future.lock().unwrap();

            if let Some(mut future) = future_slot.take() {
                let waker = waker_ref(&task);
                let context = &mut std::task::Context::from_waker(&*waker);

                if let Poll::Pending = future.as_mut().poll(context) {
                    *future_slot = Some(future);
                }
            }
        }
    }
}

#[derive(Clone)]
struct Spawner {
    task_sender: SyncSender<Arc<Task>>,
}

impl Spawner {
    fn spawn(&self, future: impl Future<Output = ()> + 'static + Send) {
        let task = Arc::new(Task {
            future: Mutex::new(Some(Box::pin(future))),
            task_sender: self.task_sender.clone(),
        });
        self.task_sender.send(task).expect("Too many requests");
    }
}

fn new_executor_and_spawner() -> (Executor, Spawner) {
    const MAX_TASKS: usize = 10_000;
    let (task_sender, ready_queue) = sync_channel(MAX_TASKS);
    (Executor { ready_queue }, Spawner { task_sender })
}

fn main() {
    let (executor, spawner) = new_executor_and_spawner();

    let mut rng = rand::thread_rng();
    let mut nums: Vec<u64> = (1..=10).collect();
    nums.shuffle(&mut rng);

    for n in 0..10 {
        let secs = *nums.get(n).unwrap_or(&1);
        spawner.spawn(async move {
            println!("Started task {} - expected runtime {} seconds", n, secs);
            TimerFuture::new(Duration::from_secs(secs)).await;
            println!("Finished task {}", n);
        });
    }

    drop(spawner);

    executor.run();
}
