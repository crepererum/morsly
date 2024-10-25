use std::{
    any::Any,
    panic::AssertUnwindSafe,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicU32, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll},
};

use datafusion::{
    arrow::{array::RecordBatch, datatypes::SchemaRef},
    common::Statistics,
    config::ConfigOptions,
    error::Result,
    execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext},
    physical_expr::LexRequirement,
    physical_plan::{
        metrics::MetricsSet, DisplayAs, DisplayFormatType, Distribution, ExecutionPlan,
        ExecutionPlanProperties, PlanProperties,
    },
};
use futures::{FutureExt, Stream, StreamExt};
use futures_concurrency::future::FutureGroup;

use super::{Graph, Node, NodeFut, NodeId};

pub(crate) fn build_graph(
    plan: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
) -> Result<(Graph, SendableRecordBatchStream)> {
    let mut builder = GraphBuilder {
        nodes: vec![],
        notify: Arc::new(AtomicU32::new(0)),
    };
    let (tx, rx) = async_channel::unbounded();
    builder.convert(None, &plan, &context, vec![tx])?;

    let graph = Graph {
        nodes: builder
            .nodes
            .into_iter()
            .map(|opt| opt.expect("all nodes converted"))
            .collect(),
        notify: builder.notify,
    };
    let output = Box::pin(ReceiverStream::new(rx, plan.schema()));
    Ok((graph, output))
}

#[derive(Debug)]
struct GraphBuilder {
    nodes: Vec<Option<Node>>,
    notify: Arc<AtomicU32>,
}

impl GraphBuilder {
    fn convert(
        &mut self,
        parent: Option<(NodeId, Arc<AtomicBool>)>,
        plan: &Arc<dyn ExecutionPlan>,
        context: &Arc<TaskContext>,
        senders: Vec<async_channel::Sender<Result<RecordBatch>>>,
    ) -> Result<()> {
        let children = plan.children();

        // allocate node ID
        let node_id = NodeId::new(self.nodes.len());
        self.nodes.push(None);

        // convert children
        let mut receivers = vec![];
        let can_run = Arc::new(AtomicBool::new(true)); // start with true to poll at least once
        for (idx, child) in children.iter().enumerate() {
            let partition_count = child.output_partitioning().partition_count();
            let mut senders = Vec::with_capacity(partition_count);
            for _ in 0..partition_count {
                let (tx, rx) = async_channel::unbounded::<Result<RecordBatch>>();
                senders.push(tx);
                receivers.push(rx);
            }

            self.convert(
                Some((node_id, Arc::clone(&can_run))),
                child,
                context,
                senders,
            )
            .map_err(|e| e.context(format!("convert child {idx} for {plan:?}")))?;
        }

        // convert self
        let mut new_children = Vec::with_capacity(children.len());
        let mut receivers_it = receivers.into_iter();
        for child in children {
            let schema = child.schema();
            let streams = (&mut receivers_it)
                .take(child.output_partitioning().partition_count())
                .map(|rx| {
                    Some(Box::pin(ReceiverStream::new(rx, Arc::clone(&schema)))
                        as SendableRecordBatchStream)
                })
                .collect();
            new_children.push(Arc::new(MorselExec {
                streams: Mutex::new(streams),
                replaced_exec: Arc::clone(child),
            }) as _);
        }
        let plan = if new_children.is_empty() {
            Arc::clone(plan)
        } else {
            Arc::clone(plan)
                .with_new_children(new_children)
                .map_err(|e| e.context(format!("replace children of {plan:?}")))?
        };

        // build driving future
        let fut = self.build_future(
            parent.as_ref().map(|p| Arc::clone(&p.1)),
            plan,
            context,
            senders,
        )?;

        // build node
        self.nodes[node_id.get()] = Some(Node {
            parent: parent.map(|p| p.0),
            fut,
            can_run,
        });

        Ok(())
    }

    fn build_future(
        &mut self,
        parent_can_run: Option<Arc<AtomicBool>>,
        plan: Arc<dyn ExecutionPlan>,
        context: &Arc<TaskContext>,
        senders: Vec<async_channel::Sender<Result<RecordBatch>>>,
    ) -> Result<NodeFut> {
        let partition_count = plan.output_partitioning().partition_count();
        assert_eq!(partition_count, senders.len());

        // group of futures that poll outputs and send it to the `senders`, i.e. towards the parents (or the final output)
        let mut fut_group = FutureGroup::with_capacity(partition_count);
        for (partition, sender) in (0..partition_count).zip(&senders) {
            let stream = plan
                .execute(partition, Arc::clone(context))
                .map_err(|e| e.context(format!("execute partition {partition} of {plan:?}")))?;
            let sender = sender.clone();
            let parent_can_run = parent_can_run.clone();
            let notify = Arc::clone(&self.notify);

            fut_group.insert(async move {
                let mut stream = stream;

                loop {
                    match stream.next().await {
                        None => {
                            return;
                        }
                        Some(x) => {
                            if sender.send(x).await.is_err() {
                                return;
                            }

                            // change "can run" status AFTER data is staged
                            // TODO(crepererum): technically we can rely on the waker to do that
                            if let Some(parent_can_run) = &parent_can_run {
                                parent_can_run.store(true, Ordering::SeqCst);

                                // modify counter AFTER setting flags
                                notify.fetch_add(1, Ordering::SeqCst);
                                atomic_wait::wake_all(notify.as_ref());
                            }
                        }
                    }
                }
            });
        }

        // poll all outputs at the same time
        let fut = async move {
            let mut fut_group = std::pin::pin!(fut_group);
            while fut_group.next().await.is_some() {}
        };

        // convert panics to errors
        let fut = async move {
            if let Err(e) = AssertUnwindSafe(fut).catch_unwind().await {
                let msg = format!("panic: {}", panic_payload_to_string(e));
                for sender in senders {
                    sender
                        .send(Err(datafusion::error::DataFusionError::Execution(
                            msg.clone(),
                        )))
                        .await
                        .ok();
                }
            }
        };

        Ok(Box::pin(fut.fuse()))
    }
}

struct MorselExec {
    streams: Mutex<Vec<Option<SendableRecordBatchStream>>>,
    replaced_exec: Arc<dyn ExecutionPlan>,
}

impl std::fmt::Debug for MorselExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MorselExec").finish_non_exhaustive()
    }
}

impl DisplayAs for MorselExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "MorselExec")
            }
        }
    }
}

impl ExecutionPlan for MorselExec {
    fn name(&self) -> &str {
        "MorselExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        self.replaced_exec.properties()
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        self.replaced_exec.required_input_distribution()
    }

    fn required_input_ordering(&self) -> Vec<Option<LexRequirement>> {
        self.replaced_exec.required_input_ordering()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        self.replaced_exec.maintains_input_order()
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        self.replaced_exec.benefits_from_input_partitioning()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        unimplemented!()
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        unimplemented!()
    }

    fn repartitioned(
        &self,
        _target_partitions: usize,
        _config: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        unimplemented!()
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let mut streams = self.streams.lock().expect("not poisoned");
        Ok(streams[partition].take().expect("stream used only once"))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.replaced_exec.metrics()
    }

    fn statistics(&self) -> Result<Statistics> {
        self.replaced_exec.statistics()
    }

    fn supports_limit_pushdown(&self) -> bool {
        self.replaced_exec.supports_limit_pushdown()
    }

    fn with_fetch(&self, _limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        unimplemented!()
    }

    fn fetch(&self) -> Option<usize> {
        self.replaced_exec.fetch()
    }
}

struct ReceiverStream {
    // TODO(crepererum): remove this allocation by proper pinning
    rx: Pin<Box<async_channel::Receiver<Result<RecordBatch>>>>,
    schema: SchemaRef,
}

impl ReceiverStream {
    fn new(rx: async_channel::Receiver<Result<RecordBatch>>, schema: SchemaRef) -> Self {
        Self {
            rx: Box::pin(rx),
            schema,
        }
    }
}

impl Stream for ReceiverStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_next_unpin(cx)
    }
}

impl RecordBatchStream for ReceiverStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

fn panic_payload_to_string(e: Box<dyn Any + Send>) -> String {
    if let Some(s) = e.downcast_ref::<String>() {
        s.clone()
    } else if let Some(s) = e.downcast_ref::<&str>() {
        (*s).to_owned()
    } else {
        "<unknown>".to_owned()
    }
}
