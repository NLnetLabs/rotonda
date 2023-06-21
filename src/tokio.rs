use futures::Future;
use tokio::task::JoinHandle;

pub(crate) fn spawn<T: Send + 'static>(
    _name: &str,
    future: impl Future<Output = T> + Send + 'static,
) -> JoinHandle<T> {
    tokio::task::spawn(future)
}
