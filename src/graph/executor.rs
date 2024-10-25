use std::{
    sync::{
        atomic::{AtomicBool, AtomicU32, Ordering},
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
        assert!(graph.nodes.is_empty());
        debug!("nothing to run!");
        return;
    };

    loop {
        if cancel.load(Ordering::SeqCst) {
            debug!("execution cancelled");
            return;
        }

        debug!("execute node: {}", next_id);
        execute_node(&mut graph.nodes[next_id.get()], Arc::clone(&graph.notify));

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
    loop {
        // get counter BEFORE checking flags
        let notify_counter = graph.notify.load(Ordering::SeqCst);

        if let Some(last_run) = last_run {
            let node = &graph.nodes[last_run.get()];

            // priority 1: the last node
            //             This mostly happens due to self-waking.
            if acquire_node_run_status(node) {
                return Some(last_run);
            }

            // priority 2: the parent node
            if let Some(id) = &node.parent {
                if acquire_node_run_status(&graph.nodes[id.get()]) {
                    return Some(*id);
                }
            }
        }

        // priority 3: full graph scan
        for (idx, node) in graph.nodes.iter().enumerate() {
            if acquire_node_run_status(node) {
                return Some(NodeId::new(idx));
            }
        }

        // nothing to do, but there might be tokio tasks or IO operations running in the background
        if graph.nodes.iter().all(|node| node.fut.is_terminated()) {
            // actually nothing to do
            return None;
        }

        // wait for something to happen
        atomic_wait::wait(&graph.notify, notify_counter);
    }
}

fn acquire_node_run_status(node: &Node) -> bool {
    node.can_run.swap(false, Ordering::SeqCst)
}

fn execute_node(node: &mut Node, notify: Arc<AtomicU32>) {
    let waker = futures::task::waker(Arc::new(Waker {
        can_run: Arc::clone(&node.can_run),
        notify,
    }));
    let mut cx = Context::from_waker(&waker);

    // ignore the output
    let _ = node.fut.poll_unpin(&mut cx);
}

struct Waker {
    can_run: Arc<AtomicBool>,
    notify: Arc<AtomicU32>,
}

impl ArcWake for Waker {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.can_run.store(true, Ordering::SeqCst);

        // modify counter AFTER setting flags
        arc_self.notify.fetch_add(1, Ordering::SeqCst);
        atomic_wait::wake_all(arc_self.notify.as_ref());
    }
}
