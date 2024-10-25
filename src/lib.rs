use std::{
    sync::{atomic::AtomicBool, Arc},
    thread::JoinHandle,
};

use datafusion::{
    arrow::{array::RecordBatch, datatypes::SchemaRef},
    error::Result,
    execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext},
    physical_plan::{
        coalesce_partitions::CoalescePartitionsExec, empty::EmptyExec, ExecutionPlan,
        ExecutionPlanProperties,
    },
};
use futures::{Stream, StreamExt};
use graph::{builder::build_graph, executor::execute_graph};

mod graph;

pub fn morsel_exec(
    plan: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
) -> Result<SendableRecordBatchStream> {
    let plan = ensure_single_output(plan);
    let (graph, output) = build_graph(plan, context)?;

    let cancel = Arc::new(AtomicBool::new(false));
    let cancel_captured = Arc::clone(&cancel);
    let handle = std::thread::spawn(move || {
        execute_graph(graph, cancel_captured);
    });
    let output = Box::pin(ExecStream {
        output,
        cancel,
        handle,
    });

    Ok(output)
}

fn ensure_single_output(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    let plan = match plan.output_partitioning().partition_count() {
        0 => Arc::new(EmptyExec::new(plan.schema())),
        1 => plan,
        2.. => Arc::new(CoalescePartitionsExec::new(plan)),
    };
    assert_eq!(1, plan.properties().output_partitioning().partition_count());
    plan
}

struct ExecStream {
    output: SendableRecordBatchStream,
    cancel: Arc<AtomicBool>,
    #[allow(dead_code)]
    handle: JoinHandle<()>,
}

impl std::fmt::Debug for ExecStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecStream")
            .field("output", &"<STREAM>")
            .field("cancel", &self.cancel)
            .field("handle", &self.handle)
            .finish()
    }
}

impl Drop for ExecStream {
    fn drop(&mut self) {
        self.cancel.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

impl Stream for ExecStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.output.poll_next_unpin(cx)
    }
}

impl RecordBatchStream for ExecStream {
    fn schema(&self) -> SchemaRef {
        self.output.schema()
    }
}
