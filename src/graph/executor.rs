use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::Context,
};

use futures::{task::ArcWake, FutureExt};
use log::debug;

use super::{Graph, Node, NodeId};

pub(crate) fn execute_graph(graph: Graph, cancel: Arc<AtomicBool>) {
    debug!("execution started");
    let mut graph = graph;
    let Some(mut next_id) = find_next_node(&graph, None) else {
        debug!("nothing to run!");
        return;
    };

    loop {
        if cancel.load(Ordering::SeqCst) {
            debug!("execution cancelled");
            return;
        }

        debug!("execute node: {}", next_id);
        execute_node(&mut graph.nodes[next_id.get()]);

        next_id = match find_next_node(&graph, Some(next_id)) {
            None => {
                debug!("execution finished");
                return;
            }
            Some(id) => id,
        };
    }
}

fn find_next_node(graph: &Graph, last_run: Option<NodeId>) -> Option<NodeId> {
    if let Some(last_run) = last_run {
        let node = &graph.nodes[last_run.get()];
        if acquire_node_run_status(node) {
            return Some(last_run);
        }
        if let Some(id) = &node.parent {
            if acquire_node_run_status(&graph.nodes[id.get()]) {
                return Some(*id);
            }
        }
    }

    for (idx, node) in graph.nodes.iter().enumerate() {
        if acquire_node_run_status(node) {
            return Some(NodeId::new(idx));
        }
    }

    None
}

fn acquire_node_run_status(node: &Node) -> bool {
    node.can_run.swap(false, Ordering::SeqCst)
}

fn execute_node(node: &mut Node) {
    let waker = futures::task::waker(Arc::new(Waker {
        can_run: Arc::clone(&node.can_run),
    }));
    let mut cx = Context::from_waker(&waker);

    // ignore the output
    let _ = node.fut.poll_unpin(&mut cx);
}

struct Waker {
    can_run: Arc<AtomicBool>,
}

impl ArcWake for Waker {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.can_run.store(true, Ordering::SeqCst);
    }
}
