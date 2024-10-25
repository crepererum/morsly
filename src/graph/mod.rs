use std::{
    pin::Pin,
    sync::{atomic::AtomicBool, Arc},
};

use futures::future::FusedFuture;

pub(crate) mod builder;
pub(crate) mod executor;

macro_rules! new_id_type {
    ($t:ident) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub(crate) struct $t(usize);

        impl $t {
            pub(crate) fn new(id: usize) -> Self {
                Self(id)
            }

            pub(crate) fn get(&self) -> usize {
                self.0
            }
        }
    };
}

new_id_type!(NodeId);

pub(crate) struct Node {
    parent: Option<NodeId>,
    fut: Pin<Box<dyn FusedFuture<Output = ()> + Send + 'static>>,
    can_run: Arc<AtomicBool>,
}

impl std::fmt::Debug for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Node")
            .field("parent", &self.parent)
            .field("fut", &"<FUT>")
            .field("can_run", &self.can_run)
            .finish()
    }
}

#[derive(Debug)]
pub(crate) struct Graph {
    nodes: Vec<Node>,
}