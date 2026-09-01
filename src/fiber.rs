use futures::future::BoxFuture;

use crate::context::Ctx;

#[derive(Debug, Clone, Copy)]
pub struct FiberId(u32);

impl FiberId {
    pub fn root() -> Self {
        Self(0)
    }

    pub fn is_root(&self) -> bool {
        self.0 == 0
    }
}

type Disposable<'a> = BoxFuture<'a, ()>;

pub struct DisposableList {
    inner: Vec<Disposable<'static>>,
}

impl DisposableList {
    pub fn defer<F>(&mut self, f: F) 
    where 
        F: Future<Output = ()> + Send + 'static
    {
        let boxed = Box::pin(f);
        self.inner.push(boxed);
    }
}

pub enum State {
    PENDING,
    FAILED,
    READY,
    DISPOSED,
}

pub struct Fiber {
    id: FiberId,
    state: State,
    context: Ctx,
    disposable: DisposableList,
}

impl Fiber {
    pub fn new(id: FiberId, context: Ctx) -> Self {
        Self {
            id,
            state: State::PENDING,
            context,
            disposable: DisposableList { inner: vec![] },
        }
    }

    pub fn fail(&mut self) {
        self.state = State::FAILED;
    }

    pub fn effect(&mut self, f: impl FnOnce(&mut DisposableList)) {
        f(&mut self.disposable)
    }

    pub async fn dispose(&mut self) {
        for dispose in self.disposable.inner.drain(..).rev() {
            dispose.await;
        }
        self.state = State::DISPOSED;
    }
}