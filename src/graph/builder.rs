use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll},
};

use datafusion::{
    arrow::{array::RecordBatch, datatypes::SchemaRef},
    error::Result,
    execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    },
};
use futures::{FutureExt, Stream, StreamExt};
use futures_concurrency::future::FutureGroup;

use super::{Graph, Node, NodeId};

pub(crate) fn build_graph(
    plan: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
) -> Result<(Graph, SendableRecordBatchStream)> {
    let mut builder = GraphBuilder::default();
    let (tx, rx) = async_channel::unbounded();
    builder.convert(None, &plan, &context, vec![tx])?;

    let graph = Graph {
        nodes: builder
            .nodes
            .into_iter()
            .map(|opt| opt.expect("all nodes converted"))
            .collect(),
    };
    let output = Box::pin(ReceiverStream::new(rx, plan.schema()));
    Ok((graph, output))
}

#[derive(Debug, Default)]
struct GraphBuilder {
    nodes: Vec<Option<Node>>,
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
        let can_run = Arc::new(AtomicBool::new(false));
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
            }) as _);
        }
        let plan = Arc::clone(plan)
            .with_new_children(new_children)
            .map_err(|e| e.context(format!("replace children of {plan:?}")))?;

        // build driving future
        let partition_count = plan.output_partitioning().partition_count();
        assert_eq!(partition_count, senders.len());
        let mut fut_group = FutureGroup::with_capacity(partition_count);
        for (partition, sender) in (0..partition_count).zip(senders) {
            let stream = plan
                .execute(partition, Arc::clone(context))
                .map_err(|e| e.context(format!("execute partition {partition} of {plan:?}")))?;
            let parent_can_run = parent.as_ref().map(|p| Arc::clone(&p.1));
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
                            }
                        }
                    }
                }
            });
        }
        let fut = async move {
            let mut fut_group = std::pin::pin!(fut_group);
            while fut_group.next().await.is_some() {}
        };

        // build node
        self.nodes[node_id.get()] = Some(Node {
            parent: parent.map(|p| p.0),
            fut: Box::pin(fut.fuse()),
            can_run,
        });

        Ok(())
    }
}

struct MorselExec {
    streams: Mutex<Vec<Option<SendableRecordBatchStream>>>,
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
        unimplemented!()
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

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let mut streams = self.streams.lock().expect("not poisoned");
        Ok(streams[partition].take().expect("stream used only once"))
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
