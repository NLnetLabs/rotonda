//! Communication between components.
//!
//! The main purpose of communication is for a unit is to announce updates to
//! its data set and operational state to all other components that are
//! interested. It also takes care of managing these communication lines.
//!
//! There are three main types here: Each unit has a single [`Gate`] to
//! which it hands its updates. The opposite end is called a [`Link`] and
//! is held by any interested component. A [`GateAgent`] is a reference to a
//! gate that can be used to create new links.
//!
//! The type [`GateMetrics`] can be used by units to provide some obvious
//! metrics such as the number of payload units in the data set or the time
//! of last update based on the updates sent to the gate.
//!
//! To connect a Link to a Gate a Subscribe command is sent via message queue
//! from the Link to the Gate using the Link `connect()` fn. Data updates are
//! broadcast from a Gate to each of its connected Links by invoking the Gate
//! `update_data()` fn. Links receive data updates via message queue. A
//! specialized type of Link called a DirectLink receives updates by direct
//! function invocation instead of by message queue, to a function registered
//! using the DirectLink variant of the `connect()` fn. Direct function
//! invocation has lower overhead and is faster, but requires the sending gate
//! to wait for the invoked function to complete before the next update can be
//! sent, i.e. the speed at which the component owning the Gate is able to
//! broadcast updates is limited by the speed of any functions registered by
//! connected DirectLinks. Conversely, a message queue allows linked
//! components to operate at different speeds which may be useful for handling
//! bursts of data, but if the publishing component is consistently faster
//! than the receiving component the message queue will become full and block
//! further updates until space becomes available again in the receive queue.
//! A Link serializes the incoming updates whereas a DirectLink can receive
//! multiple updates in parallel at the same time.
//!
//! Links receive data updates as long as they are connected to a Gate and
//! have not been suspended. Normally the Gate `get_gate_status()` fn will
//! report the Gate as Active, but if all of the Links connected to a Gate are
//! suspended the Gate is said to be Dormant, otherwise it is said to be
//! Active. A Link can be suspended via the Link `suspend()` fn. To receive
//! updates that were sent via message queue the owning component must use the
//! Link `query()` fn. Calling `query()` will unsuspend a suspended Link.
//!
//! It is not possible to publish concurrently from multiple threads via the
//! same Gate. To support components that receive data from multiple threads
//! at once without requiring them to lock the Gate in order to publish to it
//! one at a time a component can invoke the Gate `clone()` fn to give each
//! publishing thread a clone of the original Gate. However, only a subset of
//! the operations that can be performed on a Gate can be performed on a clone
//! of the Gate. If a Gate is terminated or dropped any clones of the Gate
//! will also be terminated and stop accepting updates.
//!
//! Gates do not automatically respond to commands. To process commands the
//! owning component must use the Gate `process()`, `process_until()` or
//! `wait()` functions. In addition to the basic subscribe and unsubscribe
//! commands, gates also support a few special commands. The Reconfigure
//! command is used to give feedback to the component owning a gate when the
//! configuration of the gate should be changed. The ReportLinks command is
//! used to query components for the set of Links in use by the Gate owning
//! component to receive incoming updates. Gate owning components can be
//! instructed to shutdown via the Terminate command, and Gates owning
//! components can be triggered via the Trigger command by a downstream link
//! to cause the upstream component to do something, e.g. perform a lookup
//! or calculation and pass the result back down through the Gate as a
//! QueryResult update. Additional commands are used internally to keep Gate
//! clones configuration in sync with that of the original Gate.

use crate::common::frim::FrimMap;
use crate::manager::UpstreamLinkReport;
use crate::metrics::{Metric, MetricType, MetricUnit};
use crate::tracing::Tracer;
use crate::{config::Marked, payload::Update, units::Unit};
use crate::{manager, metrics};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use crossbeam_utils::atomic::AtomicCell;
use futures::future::{select, Either, Future};
use futures::pin_mut;
use inetnum::addr::Prefix;
use log::{error, log_enabled, trace, Level};
use rotonda_store::match_options::MatchOptions;
use serde::Deserialize;
use tokio::sync::mpsc::Sender;

use std::sync::atomic::Ordering::SeqCst;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use std::{
    any::Any,
    fmt::{self, Debug, Display},
};
use std::{future::pending, sync::atomic::AtomicUsize};
use tokio::sync::{mpsc, oneshot, RwLock};
use tokio::time::{timeout_at, Instant};
use uuid::Uuid;

#[async_trait]
pub trait DirectUpdate {
    async fn direct_update(&self, update: Update);
}

pub trait AnyDirectUpdate: Any + Debug + Send + Sync + DirectUpdate {}

//------------ Configuration -------------------------------------------------

/// The queue length of an update channel.
pub const DEF_UPDATE_QUEUE_LEN: usize = 8;

/// The queue length of a command channel.
const COMMAND_QUEUE_LEN: usize = 16;

//------------ Gate ----------------------------------------------------------

#[derive(Debug)]
pub struct NormalGateState {
    /// Sender to our command receiver. Cloned when creating a clone of this
    /// Gate so that the cloned Gate can notify us when it is dropped. Only
    /// root Gates have this set, not their clones (if any).
    command_sender: mpsc::Sender<GateCommand>,

    /// Senders for propagating received commands to clones of this Gate.
    clone_senders: Arc<FrimMap<Uuid, mpsc::Sender<GateCommand>>>,
}

#[derive(Debug)]
pub struct CloneGateState {
    /// The id of this clone.
    clone_id: Uuid,

    /// A sender for sending commands to the parent of a clone, e.g. detach.
    parent_command_sender: mpsc::Sender<GateCommand>,
}

#[derive(Debug)]
pub enum GateState {
    Normal(NormalGateState),
    Clone(CloneGateState),
}

/// A communication gate representing the source of data.
///
/// Each unit receives exactly one gate. Whenever it has new data or its
/// status changes, it sends these to (through?) the gate which takes care
/// of distributing the information to whomever is interested.
///
/// A gate may be active or dormant. It is active if there is at least one
/// party interested in receiving data updates. Otherwise it is dormant.
/// Obviously, there is no need for a unit with a dormant gate to produce
/// any updates. Units are, in fact, encouraged to suspend their operation
/// until their gate becomes active again.
///
/// In order for the gate to maintain its own state, the unit needs to
/// regularly run the [`process`](Self::process) method. In return,
/// the unit will receive an update to the gate’s state as soon as it
/// becomes available.
///
/// Sending of updates happens via the [`update_data`](Self::update_data)
/// method.
#[derive(Debug)]
pub struct Gate {
    id: Arc<Mutex<Uuid>>,

    name: Arc<String>,

    /// Receiver for commands sent in by the links.
    commands: Arc<RwLock<mpsc::Receiver<GateCommand>>>,

    /// Senders to all links.
    updates: Arc<FrimMap<Uuid, UpdateSender>>,

    /// The maximum number of updates to queue per link.
    queue_size: usize,

    /// Suspended senders.
    suspended: Arc<FrimMap<Uuid, UpdateSender>>,

    /// The gate metrics.
    metrics: Arc<GateMetrics>,

    /// Gate type dependent state.
    state: GateState,

    /// Tracer
    tracer: Option<Arc<Tracer>>,
}

// On drop, notify the parent of a cloned gate that this clone is detaching
// itself so that the parent Gate remove the corresponding entry from its
// clone senders collection, otherwise clones "leak" memory in the parent
// because a reference to them is never cleaned up.
impl Drop for Gate {
    fn drop(&mut self) {
        if log_enabled!(Level::Trace) {
            let clone_txt = if self.is_clone() {
                format!("{} clone of ", self.clone_id())
            } else {
                String::new()
            };
            trace!("Gate[{} ({}{})]: Drop", self.name, clone_txt, self.id());
        }
        if self.is_clone() {
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                tokio::task::block_in_place(move || {
                    handle.block_on(self.detach());
                });
            } else {
                self.blocking_detach();
            }
        }
    }
}

impl Default for Gate {
    fn default() -> Self {
        Self::new(DEF_UPDATE_QUEUE_LEN).0
    }
}

impl Gate {
    /// Creates a new gate.
    ///
    /// The function returns a gate and a gate agent that allows creating new
    /// links. Typically, you would pass the gate to a subsequently created
    /// unit and keep the agent around for future use.
    pub fn new(queue_size: usize) -> (Gate, GateAgent) {
        let (tx, rx) = mpsc::channel(COMMAND_QUEUE_LEN);
        let gate = Gate {
            id: Arc::new(Mutex::new(Uuid::new_v4())),
            name: Arc::default(),
            commands: Arc::new(RwLock::new(rx)),
            updates: Default::default(),
            queue_size,
            suspended: Default::default(),
            metrics: Default::default(),
            state: GateState::Normal(NormalGateState {
                command_sender: tx.clone(),
                clone_senders: Default::default(),
            }),
            tracer: None,
        };
        let agent = GateAgent {
            id: gate.id.clone(),
            commands: tx,
        };
        if log_enabled!(Level::Trace) {
            trace!("Gate[{} ({})]: New gate created", gate.name, gate.id());
        }
        (gate, agent)
    }

    /// Take the key internals of a Gate to use elsewhere.
    ///
    /// Can't be done manually via destructuring due to the existence of the
    /// Drop impl for Gate.
    ///
    /// For internal use only, hence not public.
    fn take(
        self,
    ) -> (mpsc::Receiver<GateCommand>, FrimMap<Uuid, UpdateSender>) {
        let commands = self.commands.clone();
        let updates = self.updates.clone();
        drop(self);
        let commands = Arc::try_unwrap(commands).unwrap().into_inner();
        let updates = Arc::try_unwrap(updates).unwrap();
        (commands, updates)
    }

    pub fn id(&self) -> Uuid {
        *self.id.lock().unwrap()
    }

    pub fn is_clone(&self) -> bool {
        match self.state {
            GateState::Normal(_) => false,
            GateState::Clone(_) => true,
        }
    }

    fn clone_id(&self) -> Uuid {
        let GateState::Clone(CloneGateState { clone_id, .. }) = self.state
        else {
            unreachable!()
        };
        clone_id
    }

    /// Returns a shareable reference to the gate metrics.
    ///
    /// Metrics are shared between a gate and its clones.
    pub fn metrics(&self) -> Arc<GateMetrics> {
        self.metrics.clone()
    }

    pub async fn detach(&self) {
        if let GateState::Clone(CloneGateState {
            clone_id,
            parent_command_sender,
            ..
        }) = &self.state
        {
            if log_enabled!(Level::Trace) {
                let clone_txt = format!("{clone_id} clone of ");
                trace!(
                    "Gate[{} ({}{})]: Detach",
                    self.name,
                    clone_txt,
                    self.id()
                );
            }
            if let Err(_err) = parent_command_sender
                .send(GateCommand::DetachClone {
                    clone_id: *clone_id,
                })
                .await
            {
                // TODO
            }
        } else {
            error!(
                "Gate[{} ({})]: Root gates cannot be detached!",
                self.name,
                self.id()
            );
        }
    }

    pub fn blocking_detach(&self) {
        if let GateState::Clone(CloneGateState {
            clone_id,
            parent_command_sender,
            ..
        }) = &self.state
        {
            if log_enabled!(Level::Trace) {
                let clone_txt = format!("{clone_id} clone of ");
                trace!(
                    "Gate[{} ({}{})]: Blocking detach",
                    self.name,
                    clone_txt,
                    self.id()
                );
            }
            if let Err(_err) = parent_command_sender.blocking_send(
                GateCommand::DetachClone {
                    clone_id: *clone_id,
                },
            ) {
                // TODO
            }
        } else {
            error!(
                "Gate[{} ({})]: Root gates cannot be blocking detached!",
                self.name,
                self.id()
            );
        }
    }

    pub fn set_tracer(&mut self, tracer: Arc<Tracer>) {
        self.tracer = Some(tracer);
    }

    /// Runs the gate’s internal machine.
    ///
    /// This method returns a future that runs the gate’s internal machine.
    /// It resolves once the gate’s status changes. It can be dropped at any
    /// time. In this case, the gate will pick up where it left off when the
    /// method is called again.
    ///
    /// The method will resolve into an error if the unit should terminate.
    /// This is the case if all links and gate agents referring to the gate
    /// have been dropped.
    ///
    /// # Panics
    ///
    /// Panics if a cloned gate receives `GateCommand::Subscribe` or a
    /// non-cloned gate receives `GateCommand::FollowSubscribe`.
    pub async fn process(&self) -> Result<GateStatus, Terminated> {
        let status = self.get_gate_status();
        loop {
            let command = {
                let mut lock = self.commands.write().await;
                match lock.recv().await {
                    Some(command) => command,
                    None => {
                        if log_enabled!(Level::Trace) {
                            let clone_txt = if self.is_clone() {
                                format!("{} clone of ", self.clone_id())
                            } else {
                                String::new()
                            };
                            trace!(
                                "Gate[{} ({}{})]: Command channel has been closed",
                                self.name,
                                clone_txt,
                                self.id()
                            );
                        }
                        return Err(Terminated);
                    }
                }
            };

            if log_enabled!(Level::Trace) {
                let clone_txt = if self.is_clone() {
                    format!("{} clone of ", self.clone_id())
                } else {
                    String::new()
                };
                trace!(
                    "Gate[{} ({}{})]: Received command '{}'",
                    self.name,
                    clone_txt,
                    self.id(),
                    command
                );
            }

            match command {
                GateCommand::AttachClone { clone_id, tx } => {
                    match &self.state {
                        GateState::Normal(state) => {
                            state.clone_senders.insert(clone_id, tx);
                        }
                        GateState::Clone(_) => unreachable!(),
                    }
                }

                GateCommand::DetachClone { clone_id } => match &self.state {
                    GateState::Normal(state) => {
                        let _ = state.clone_senders.remove(&clone_id);
                    }
                    GateState::Clone(_) => self.detach().await,
                },

                GateCommand::Suspension { slot, suspend } => {
                    self.suspension(slot, suspend)
                }

                GateCommand::Subscribe {
                    suspended,
                    response,
                    direct_update,
                } => {
                    assert!(
                        !self.is_clone(),
                        "Cloned gates do not support the Subscribe command"
                    );
                    self.subscribe(suspended, response, direct_update).await
                }

                GateCommand::Unsubscribe { slot } => {
                    assert!(
                        !self.is_clone(),
                        "Cloned gates do not support the Unsubscribe command"
                    );
                    self.unsubscribe(slot).await
                }

                GateCommand::FollowSubscribe {
                    slot,
                    update_sender,
                } => {
                    assert!(
                        self.is_clone(),
                        "Only cloned gates support the FollowSubscribe command"
                    );
                    self.updates.insert(slot, update_sender);
                }

                GateCommand::FollowUnsubscribe { slot } => {
                    assert!(
                        self.is_clone(),
                        "Only cloned gates support the FollowUnsubscribe command"
                    );
                    self.updates.remove(&slot);
                }

                GateCommand::Reconfigure {
                    new_config,
                    new_gate,
                } => {
                    assert!(
                        !self.is_clone(),
                        "Cloned gates do not support the Reconfigure command"
                    );

                    // Ensure we drop the lock before we hit the .awaits below
                    // as we cannot hold the lock across an .await.
                    {
                        let new_id = *new_gate.id.lock().unwrap();
                        let mut id = self.id.lock().unwrap();
                        if log_enabled!(log::Level::Trace) {
                            trace!(
                                "Gate[{} ({})]: Reconfiguring: new ID={}",
                                self.name,
                                id,
                                new_id
                            );
                        }
                        *id = new_id;
                    }

                    // This is an ugly way to take over the internals of the
                    // given Gate object and use them ourselves. We need to
                    // take() because just destructuring the Gate struct
                    // causes compilation failure because the Gate innards
                    // can't be moved out when Gate has a Drop impl.
                    let (new_commands, new_updates) = new_gate.take();
                    *self.commands.write().await = new_commands;
                    self.updates.replace(new_updates);
                    self.notify_clones(GateCommand::FollowReconfigure {
                        new_config: new_config.clone(),
                    })
                    .await;
                    return Ok(GateStatus::Reconfiguring { new_config });
                }

                GateCommand::FollowReconfigure { new_config } => {
                    assert!(
                        self.is_clone(),
                        "Only cloned gates support the FollowReconfigure command"
                    );
                    return Ok(GateStatus::Reconfiguring { new_config });
                }

                GateCommand::ReportLinks { report } => {
                    self.notify_clones(GateCommand::ReportLinks {
                        report: report.clone(),
                    })
                    .await;
                    return Ok(GateStatus::ReportLinks { report });
                }

                GateCommand::Trigger { data } => {
                    self.notify_clones(GateCommand::Trigger {
                        data: data.clone(),
                    })
                    .await;
                    return Ok(GateStatus::Triggered { data });
                }

                GateCommand::Terminate => {
                    self.notify_clones(GateCommand::Terminate).await;
                    return Err(Terminated);
                }
            }

            let new_status = self.get_gate_status();
            if new_status != status {
                return Ok(new_status);
            }
        }
    }

    async fn notify_clones(&self, cmd: GateCommand) {
        if let GateState::Normal(NormalGateState { clone_senders, .. }) =
            &self.state
        {
            let mut closed_sender_found = false;
            for (uuid, sender) in clone_senders.guard().iter() {
                if !sender.is_closed() {
                    if log_enabled!(Level::Trace) {
                        let clone_txt = if self.is_clone() {
                            format!("{} clone of ", self.clone_id())
                        } else {
                            String::new()
                        };
                        trace!(
                            "Gate[{} ({}{})]: Notifying clone {} of command '{}'",
                            self.name,
                            clone_txt,
                            self.id(),
                            uuid,
                            cmd
                        );
                    }
                    sender.send(cmd.clone()).await.expect(
                        "Internal error: failed to notify cloned gate",
                    );
                } else {
                    if log_enabled!(Level::Trace) {
                        let clone_txt = if self.is_clone() {
                            format!("{} clone of ", self.clone_id())
                        } else {
                            String::new()
                        };
                        trace!("Gate[{} ({}{})]: Unable to notify clone {} of command '{}': sender is closed", self.name, clone_txt, self.id(), uuid, cmd);
                    }
                    closed_sender_found = true;
                }
            }

            if closed_sender_found {
                clone_senders.retain(|_uuid, sender| !sender.is_closed());
            }
        }
    }

    /// Runs the gate’s internal machine until a future resolves.
    ///
    /// Ignores any gate status changes.
    ///
    /// # Panics
    ///
    /// See [process()](Self::process).
    pub async fn process_until<Fut: Future>(
        &self,
        fut: Fut,
    ) -> Result<Fut::Output, Terminated> {
        pin_mut!(fut);

        loop {
            let process = self.process();
            pin_mut!(process);
            match select(process, fut).await {
                Either::Left((Err(_), _)) => return Err(Terminated),
                Either::Left((Ok(_), next_fut)) => {
                    fut = next_fut;
                }
                Either::Right((res, _)) => return Ok(res),
            }
        }
    }

    /// Runs the gate's internal machine for a period of time.
    ///
    /// # Panics
    ///
    /// See [process()](Self::process).
    pub async fn wait(&self, secs: u64) -> Result<(), Terminated> {
        let end = Instant::now() + Duration::from_secs(secs);

        while end > Instant::now() {
            match timeout_at(end, self.process()).await {
                Ok(Ok(_status)) => {
                    // Wait interrupted by internal gate status change, keep
                    // waiting
                }
                Ok(Err(Terminated)) => {
                    // Wait interrupted by gate termination, abort
                    return Err(Terminated);
                }
                Err(_) => {
                    // Wait completed
                    return Ok(());
                }
            }
        }

        // Wait end time passed
        Ok(())
    }

    /// Updates the data set of the unit.
    ///
    /// This method will send out the update to all active links. It will
    /// also update the gate metrics based on the update.
    ///
    /// Returns true if the update was sent to a downstream unit, false
    /// otherwise.
    pub async fn update_data(&self, update: Update) {
        // let mut sender_lost = false;
        let mut sent_at_least_once = false;

        if log_enabled!(Level::Trace) {
            let clone_txt = if self.is_clone() {
                format!("{} clone of ", self.clone_id())
            } else {
                String::new()
            };
            trace!(
                "Gate[{} ({}{})]: Starting update",
                self.name,
                clone_txt,
                self.id()
            );
        }
        for (uuid, item) in self.updates.guard().iter() {
            match (&item.queue, &item.direct) {
                (Some(sender), None) => {
                    if let Some(tracer) = &self.tracer {
                        for payload in update.trace_ids() {
                            tracer.note_gate_event(
                                    payload.trace_id().unwrap(),
                                    self.id(),
                                    format!("Sent by queue from gate {} to slot {uuid}: {payload:#?}", self.id()),
                                );
                        }
                    }
                    if sender.send(Ok(update.clone())).await.is_ok() {
                        sent_at_least_once = true;
                        continue;
                    }
                }
                (None, Some(direct)) => {
                    if log_enabled!(log::Level::Trace) {
                        let clone_txt = if self.is_clone() {
                            format!("{} clone of ", self.clone_id())
                        } else {
                            String::new()
                        };
                        trace!(
                                "Gate[{} ({}{})]: Sending direct update for slot {}",
                                self.name,
                                clone_txt,
                                self.id(),
                                uuid
                            );
                    }
                    if let Some(tracer) = &self.tracer {
                        for payload in update.trace_ids() {
                            tracer.note_gate_event(
                                    payload.trace_id().unwrap(),
                                    self.id(),
                                    format!(
                                        "Sent by direct update from gate {} to slot {uuid}: {payload:#?}",
                                        self.id()
                                    ),
                                );
                        }
                    }
                    if let Some(direct) = direct.upgrade() {
                        direct.direct_update(update.clone()).await;
                        sent_at_least_once = true;
                    }
                    continue;
                }
                _ => {}
            }
            // We don't actually have any usage of queue based sending at
            // present so we can skip doing this for now. item.queue = None;
            // sender_lost = true;
        }
        if log_enabled!(Level::Trace) {
            let clone_txt = if self.is_clone() {
                format!("{} clone of ", self.clone_id())
            } else {
                String::new()
            };
            trace!(
                "Gate[{} ({}{})]: Finished update",
                self.name,
                clone_txt,
                self.id()
            );
        }

        // if sender_lost {
        //     let updates = self.updates.load();
        //     updates.retain(|_, item| item.queue.is_some());
        //     self.updates_len.store(updates.len(), SeqCst);
        // }

        self.metrics.update(
            &update,
            self.updates.clone(),
            sent_at_least_once,
        );
    }

    /// Returns the current gate status.
    pub fn get_gate_status(&self) -> GateStatus {
        if self.suspended.len() == self.updates.len() {
            GateStatus::Dormant
        } else {
            GateStatus::Active
        }
    }

    /// Processes a suspension command.
    fn suspension(&self, slot: Uuid, suspend: bool) {
        if suspend {
            if let Some(removed) = self.updates.remove(&slot) {
                self.suspended.insert(slot, removed);
            }
        } else if let Some(removed) = self.suspended.remove(&slot) {
            self.updates.insert(slot, removed);
        }
    }

    /// Processes a subscribe command.
    ///
    /// Clones of the gate will receive `GateCommand::FollowSubscribe` to keep
    /// their set of update senders in sync with the original gate.
    async fn subscribe(
        &self,
        suspended: bool,
        response: oneshot::Sender<SubscribeResponse>,
        direct_update: Option<Weak<dyn AnyDirectUpdate>>,
    ) {
        let (update_sender, receiver) =
            if let Some(direct_update) = direct_update {
                let update_sender = UpdateSender {
                    queue: None,
                    direct: Some(direct_update),
                };
                (update_sender, None)
            } else {
                let (tx, receiver) = mpsc::channel(self.queue_size);
                let update_sender = UpdateSender {
                    queue: Some(tx),
                    direct: None,
                };
                (update_sender, Some(receiver))
            };

        let slot = Uuid::new_v4();
        if suspended {
            self.suspended.insert(slot, update_sender.clone());
        } else {
            self.updates.insert(slot, update_sender.clone());
        }

        let subscription = SubscribeResponse { slot, receiver };

        if let Err(subscription) = response.send(subscription) {
            if suspended {
                self.suspended.remove(&subscription.slot);
            } else {
                self.updates.remove(&subscription.slot);
            }
        } else {
            self.notify_clones(GateCommand::FollowSubscribe {
                slot,
                update_sender,
            })
            .await;
        }
    }

    async fn unsubscribe(&self, slot: Uuid) {
        self.suspended.remove(&slot);
        self.updates.remove(&slot);
        self.notify_clones(GateCommand::FollowUnsubscribe { slot })
            .await;
    }

    pub(crate) fn set_name(&mut self, name: &str) {
        self.name = Arc::new(name.to_string());
    }

    pub fn name(&self) -> Arc<String> {
        self.name.clone()
    }
}

impl Clone for Gate {
    /// Clone the gate.
    ///
    /// # Why clone?
    ///
    /// Cloning a gate clones the underlying mpsc::Sender instances so that the
    /// gate can be passed across await/thread boundaries in order for multiple
    /// tasks to concurrently push updates through the gate.
    ///
    /// A default Clone impl isn't possible because the command receiver cannot
    /// be cloned. Instead we give the clone its own command receiver and give
    /// the corresponding sender to the parent so that commands relevant to the
    /// clone can be sent by the parent to the clone.
    fn clone(&self) -> Self {
        let (tx, rx) = mpsc::channel(COMMAND_QUEUE_LEN);

        let clone_id = Uuid::new_v4();

        if log_enabled!(Level::Trace) {
            let clone_txt = if self.is_clone() {
                format!("{} clone of ", self.clone_id())
            } else {
                String::new()
            };
            trace!(
                "Gate[{} ({}{})]: Cloning gate to new clone id {}",
                self.name,
                clone_txt,
                self.id(),
                clone_id
            );
        }

        let parent_command_sender = match &self.state {
            GateState::Normal(state) => state.command_sender.clone(),
            GateState::Clone(state) => state.parent_command_sender.clone(),
        };

        let gate = Gate {
            id: self.id.clone(),
            name: self.name.clone(),
            commands: Arc::new(RwLock::new(rx)),
            updates: self.updates.clone(),
            queue_size: self.queue_size,
            suspended: self.suspended.clone(),
            metrics: self.metrics.clone(),
            state: GateState::Clone(CloneGateState {
                clone_id,
                parent_command_sender: parent_command_sender.clone(),
            }),
            tracer: self.tracer.clone(),
        };

        // Ask the real gate to add our command sender to the set it sends
        // command notifications to
        let cloned_name = self.name.clone();
        let copied_id = self.id();
        crate::tokio::spawn("gate-attach-clone", async move {
            let saved_clone_id = clone_id;
            if let Err(_err) = parent_command_sender
                .send(GateCommand::AttachClone { clone_id, tx })
                .await
            {
                let clone_txt = format!("{} clone of ", saved_clone_id);
                error!(
                    "Gate[{} ({}{})]: Failed to attach clone to parent {}",
                    cloned_name, clone_txt, copied_id, clone_id
                );
            }
        });

        gate
    }
}

//------------ GateAgent -----------------------------------------------------

/// A representative of a gate allowing creation of new links for it.
///
/// The agent can be cloned and passed along. The method
/// [`create_link`](Self::create_link) can be used to create a new link.
///
/// Yes, the name is a bit of a mixed analogy.
#[derive(Clone, Debug)]
pub struct GateAgent {
    id: Arc<Mutex<Uuid>>,
    commands: mpsc::Sender<GateCommand>,
}

impl GateAgent {
    pub fn id(&self) -> Uuid {
        *self.id.lock().unwrap()
    }

    /// Creates a new link to the gate.
    pub fn create_link(&mut self) -> Link {
        Link::new(self.id(), self.commands.clone())
    }

    pub async fn terminate(&self) {
        let _ = self.commands.send(GateCommand::Terminate).await;
    }

    pub fn is_terminated(&self) -> bool {
        self.commands.is_closed()
    }

    pub async fn reconfigure(
        &self,
        new_config: Unit,
        new_gate: Gate,
    ) -> Result<(), String> {
        self.commands
            .send(GateCommand::Reconfigure {
                new_config,
                new_gate,
            })
            .await
            .map_err(|err| format!("{}", err))
    }

    pub async fn report_links(
        &self,
        report: UpstreamLinkReport,
    ) -> Result<(), String> {
        self.commands
            .send(GateCommand::ReportLinks { report })
            .await
            .map_err(|err| format!("{}", err))
    }
}

//------------ GraphMetrics --------------------------------------------------
pub trait GraphStatus: Send + Sync {
    fn status_text(&self) -> String;

    fn okay(&self) -> Option<bool> {
        None
    }
}

//------------ GateMetrics ---------------------------------------------------

/// Metrics about the updates distributed via the gate.
///
/// This type is a [`metrics::Source`](crate::metrics::Source) that provides a
/// number of metrics for a unit that can be derived from the updates sent by
/// the unit and thus are common to all units.
///
/// Gates provide access to values of this type via the [`Gate::metrics`]
/// method. When stored behind an arc t can be kept and passed around freely.
#[derive(Debug, Default)]
pub struct GateMetrics {
    /// The number of payload items in the last update.
    pub update_set_size: AtomicUsize,

    /// The date and time of the last update.
    ///
    /// If there has never been an update, this will be `None`.
    pub update: AtomicCell<Option<DateTime<Utc>>>,

    /// The number of updates sent through the gate
    pub num_updates: AtomicUsize,

    /// The number of updates that could not be sent through the gate
    pub num_dropped_updates: AtomicUsize,
}

impl GraphStatus for GateMetrics {
    fn status_text(&self) -> String {
        format!("out: {}", self.num_updates.load(SeqCst))
    }
}

impl GateMetrics {
    /// Updates the metrics to match the given update.
    fn update(
        &self,
        update: &Update,
        _senders: Arc<FrimMap<Uuid, UpdateSender>>,
        sent_at_least_once: bool,
    ) {
        self.num_updates.fetch_add(1, SeqCst);
        if !sent_at_least_once {
            self.num_dropped_updates.fetch_add(1, SeqCst);
        }
        if let Update::Bulk(update) = update {
            self.update_set_size.store(update.len(), SeqCst);
        }
        self.update.store(Some(Utc::now()));
    }
}

impl GateMetrics {
    const NUM_UPDATES_METRIC: Metric = Metric::new(
        "num_updates",
        "the number of updates sent through the gate",
        MetricType::Counter,
        MetricUnit::Total,
    );
    const NUM_DROPPED_UPDATES_METRIC: Metric = Metric::new(
        "num_dropped_updates",
        "the number of updates that could not be sent through the gate",
        MetricType::Counter,
        MetricUnit::Total,
    );
    const UPDATE_SET_SIZE_METRIC: Metric = Metric::new(
        "update_set_size",
        "the number of set items in the last update",
        MetricType::Gauge,
        MetricUnit::Total,
    );
    const UPDATE_WHEN_METRIC: Metric = Metric::new(
        "last_update",
        "the date and time of the last update",
        MetricType::Text,
        MetricUnit::Info,
    );
    const UPDATE_AGO_METRIC: Metric = Metric::new(
        "since_last_update",
        "the number of seconds since the last update",
        MetricType::Gauge,
        MetricUnit::Second,
    );
}

impl metrics::Source for GateMetrics {
    /// Appends the current gate metrics to a target.
    ///
    /// The name of the unit these metrics are associated with is given via
    /// `unit_name`.
    fn append(&self, unit_name: &str, target: &mut metrics::Target) {
        target.append_simple(
            &Self::NUM_UPDATES_METRIC,
            Some(unit_name),
            self.num_updates.load(SeqCst),
        );

        target.append_simple(
            &Self::NUM_DROPPED_UPDATES_METRIC,
            Some(unit_name),
            self.num_dropped_updates.load(SeqCst),
        );

        match self.update.load() {
            Some(update) => {
                target.append_simple(
                    &Self::UPDATE_WHEN_METRIC,
                    Some(unit_name),
                    update,
                );
                let ago =
                    Utc::now().signed_duration_since(update).num_seconds();
                target.append_simple(
                    &Self::UPDATE_AGO_METRIC,
                    Some(unit_name),
                    ago,
                );

                target.append_simple(
                    &Self::UPDATE_SET_SIZE_METRIC,
                    Some(unit_name),
                    self.update_set_size.load(SeqCst),
                );
            }
            None => {
                target.append_simple(
                    &Self::UPDATE_WHEN_METRIC,
                    Some(unit_name),
                    "N/A",
                );
                target.append_simple(
                    &Self::UPDATE_AGO_METRIC,
                    Some(unit_name),
                    -1,
                );
            }
        }
    }
}

//------------ DirectLink ----------------------------------------------------

/// A direct link to a unit.
///
/// Like [Link] but data updates are sent directly from the linked gate to the
/// fn registered when `connect()` is invoked. Direct updates can be orders of
/// magnitude faster than normal queue based links, especially when multiple
/// concurrent writers to the gate are possible. However, unlike a normal
/// queue based link where delays at the link owner don't immediately impact
/// the gate owner, with a direct link any delay in the link owner will block
/// the sending gate thread/task.
#[derive(Clone, Debug, Deserialize)]
#[serde(from = "String")]
pub struct DirectLink(Link);

impl DirectLink {
    pub fn id(&self) -> Uuid {
        self.0.id()
    }

    pub fn gate_id(&self) -> Uuid {
        self.0.gate_id()
    }

    pub fn connected_gate_slot(&self) -> Option<Uuid> {
        self.0.connection.as_ref().map(|connection| connection.slot)
    }

    /// Suspends the link.
    ///
    /// A suspended link will not receive any payload updates from the
    /// connected unit. It will, however, still receive status updates.
    ///
    /// The suspension is lifted automatically the next time `query` is
    /// called.
    ///
    /// Note that this is an async method that needs to be awaited in order
    /// to do anything.
    pub async fn suspend(&mut self) {
        self.0.suspend().await
    }

    /// Returns the current status of the connected unit.
    pub fn get_status(&self) -> UnitStatus {
        self.0.get_status()
    }

    /// Connects the link to the gate.
    ///
    /// MUST be invoked before the link can receive data updates from the
    /// gate.
    pub async fn connect(
        &mut self,
        direct_update_target: Arc<dyn AnyDirectUpdate>,
        suspended: bool,
    ) -> Result<(), UnitStatus> {
        self.0.set_direct_update_target(direct_update_target);
        self.0.connect(suspended).await
    }

    pub async fn disconnect(&mut self) {
        self.0.direct_update_target = None;
        self.0.disconnect().await;
    }
}

impl From<Link> for DirectLink {
    fn from(link: Link) -> Self {
        DirectLink(link)
    }
}

impl From<Marked<String>> for DirectLink {
    fn from(name: Marked<String>) -> Self {
        DirectLink(manager::load_link(name))
    }
}

impl From<String> for DirectLink {
    fn from(name: String) -> Self {
        DirectLink(manager::load_link(name.into()))
    }
}

//------------ Link ----------------------------------------------------------

#[derive(Debug)]
struct LinkConnection {
    /// The slot number at the gate.
    slot: Uuid,

    /// The update receiver.
    updates: Option<UpdateReceiver>,
}

/// A queued link to a unit.
///
/// The link allows tracking of updates of that other unit. This happens via
/// the [`query`](Self::query) method. A link’s owner can signal that they
/// are currently not interested in receiving updates via the
/// [`suspend`](Self::suspend) method. This suspension will automatically be
/// lifted the next time `query` is called.
///
/// Links can be created from the name of the unit they should be linking to
/// via [manager::load_link](crate::manager::load_link). This function is
/// also called implicitly through the impls for `Deserialize` and `From`.
/// Note, however, that the function only adds the link to a list of links
/// to be properly connected by the manager later.
#[derive(Deserialize)]
#[serde(from = "String")]
pub struct Link {
    id: Uuid,

    gate_id: Uuid,

    /// A sender of commands to the gate.
    commands: mpsc::Sender<GateCommand>,

    /// The connection to the unit.
    ///
    /// If this is `None`, the link has not been connected yet.
    connection: Option<LinkConnection>,

    /// The current unit status.
    unit_status: UnitStatus,

    /// Are we currently suspended?
    suspended: bool,

    direct_update_target: Option<Weak<dyn AnyDirectUpdate>>,
}

impl PartialEq for Link {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Link {}

impl std::fmt::Debug for Link {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Link")
            .field("id", &self.id)
            .field("gate_id", &self.gate_id)
            .field("connection", &self.connected_gate_slot())
            .field("unit_status", &self.unit_status)
            .field("suspended", &self.suspended)
            .field(
                "direct_update_target",
                &self.direct_update_target.is_some(),
            )
            .finish()
    }
}

impl Clone for Link {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            gate_id: self.gate_id,
            commands: self.commands.clone(),
            connection: None,
            unit_status: self.unit_status,
            suspended: self.suspended,
            direct_update_target: self.direct_update_target.clone(),
        }
    }
}

impl Link {
    /// Creates a new, unconnected link.
    fn new(gate_id: Uuid, commands: mpsc::Sender<GateCommand>) -> Self {
        Link {
            id: Uuid::new_v4(),
            gate_id,
            commands,
            connection: None,
            unit_status: UnitStatus::Healthy,
            suspended: false,
            direct_update_target: None,
        }
    }

    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn gate_id(&self) -> Uuid {
        self.gate_id
    }

    pub fn connected_gate_slot(&self) -> Option<Uuid> {
        self.connection.as_ref().map(|connection| connection.slot)
    }

    pub fn close(&mut self) {
        if let Some(conn) = self.connection.as_mut() {
            if let Some(updates) = &mut conn.updates {
                updates.close();
            }
        }
    }

    /// Query for the next update.
    ///
    /// The method returns a future that resolves into the next update. The
    /// future can be dropped safely at any time.
    ///
    /// The future either resolves into a payload update or the connected
    /// unit’s new status as the error variant. The current status is also
    /// available via the `get_status` method.
    ///
    /// If the link is currently suspended, calling this method will lift the
    /// suspension.
    pub async fn query(&mut self) -> Result<Update, UnitStatus> {
        self.connect(false).await?;
        let conn = self.connection.as_mut().unwrap();

        if let Some(updates) = &mut conn.updates {
            match updates.recv().await {
                Some(Ok(update)) => Ok(update),
                Some(Err(status)) => {
                    self.unit_status = status;
                    Err(status)
                }
                None => {
                    self.unit_status = UnitStatus::Gone;
                    Err(UnitStatus::Gone)
                }
            }
        } else {
            pending().await
        }
    }

    /// Query a suspended link.
    ///
    /// When a link is suspended, it still received updates to the unit’s
    /// status. These updates can also be queried for explicitly via this
    /// method.
    ///
    /// Much like `query`, the future returned by this method can safely be
    /// dropped at any time.
    pub async fn query_suspended(&mut self) -> UnitStatus {
        if let Err(err) = self.connect(true).await {
            return err;
        }
        let conn = self.connection.as_mut().unwrap();

        if let Some(updates) = &mut conn.updates {
            loop {
                match updates.recv().await {
                    Some(Ok(_)) => continue,
                    Some(Err(status)) => return status,
                    None => {
                        self.unit_status = UnitStatus::Gone;
                        return UnitStatus::Gone;
                    }
                }
            }
        } else {
            pending().await
        }
    }

    /// Suspends the link.
    ///
    /// A suspended link will not receive any payload updates from the
    /// connected unit. It will, however, still receive status updates.
    ///
    /// The suspension is lifted automatically the next time `query` is
    /// called.
    ///
    /// Note that this is an async method that needs to be awaited in order
    /// to do anything.
    pub async fn suspend(&mut self) {
        if !self.suspended {
            self.request_suspend(true).await
        }
    }

    /// Request suspension from the gate.
    async fn request_suspend(&mut self, suspend: bool) {
        if self.connection.is_none() {
            return;
        }

        let conn = self.connection.as_mut().unwrap();
        if self
            .commands
            .send(GateCommand::Suspension {
                slot: conn.slot,
                suspend,
            })
            .await
            .is_err()
        {
            self.unit_status = UnitStatus::Gone
        } else {
            self.suspended = suspend
        }
    }

    /// Returns the current status of the connected unit.
    pub fn get_status(&self) -> UnitStatus {
        self.unit_status
    }

    /// Connects the link to the gate.
    pub async fn connect(
        &mut self,
        suspended: bool,
    ) -> Result<(), UnitStatus> {
        if self.connection.is_some() {
            return Ok(());
        }
        if let UnitStatus::Gone = self.unit_status {
            return Err(UnitStatus::Gone);
        }

        let (tx, rx) = oneshot::channel();
        if self
            .commands
            .send(GateCommand::Subscribe {
                suspended,
                response: tx,
                direct_update: self.direct_update_target.clone(),
            })
            .await
            .is_err()
        {
            self.unit_status = UnitStatus::Gone;
            return Err(UnitStatus::Gone);
        }
        let sub = match rx.await {
            Ok(sub) => sub,
            Err(_) => {
                self.unit_status = UnitStatus::Gone;
                return Err(UnitStatus::Gone);
            }
        };
        self.connection = Some(LinkConnection {
            slot: sub.slot,
            updates: sub.receiver,
        });
        if log_enabled!(log::Level::Trace) {
            trace!(
                "Link[{}]: connected to gate slot {}",
                self.id(),
                sub.slot
            );
        }
        self.unit_status = UnitStatus::Healthy;
        self.suspended = suspended;
        Ok(())
    }

    /// Disconnects the link to the gate
    pub async fn disconnect(&mut self) {
        if let Some(connection) = &self.connection {
            let _ = self
                .commands
                .send(GateCommand::Unsubscribe {
                    slot: connection.slot,
                })
                .await;
            if log_enabled!(log::Level::Trace) {
                trace!(
                    "Link[{}]: disconnected from gate slot {}",
                    self.id(),
                    connection.slot
                );
            }
        }
        self.connection = None;
    }

    /// Trigger an upstream unit to do something.
    pub async fn trigger(&self, data: TriggerData) {
        let _ = self.commands.send(GateCommand::Trigger { data }).await;
        if log_enabled!(log::Level::Trace) {
            trace!("Link[{}]: sent trigger to gate slot", self.id(),);
        }
    }

    /// See [DirectLink].
    pub fn set_direct_update_target(
        &mut self,
        direct_update_target: Arc<dyn AnyDirectUpdate>,
    ) {
        self.direct_update_target =
            Some(Arc::downgrade(&direct_update_target));
    }
}

impl Drop for Link {
    fn drop(&mut self) {
        if let Some(connection) = &self.connection {
            let id = self.id();
            let slot = connection.slot;
            let tx = self.commands.clone();
            crate::tokio::spawn("drop-link", async move {
                let _ = tx.send(GateCommand::Unsubscribe { slot }).await;
                if log_enabled!(log::Level::Trace) {
                    trace!(
                        "Link[{}]: disconnected from gate slot {} on drop",
                        id,
                        slot
                    );
                }
            });
        }
    }
}

impl From<Marked<String>> for Link {
    fn from(name: Marked<String>) -> Self {
        manager::load_link(name)
    }
}

impl From<String> for Link {
    fn from(name: String) -> Self {
        manager::load_link(name.into())
    }
}

//------------ GateStatus ----------------------------------------------------

/// The status of a gate.
#[derive(Debug, Default)]
pub enum GateStatus {
    /// The gate is connected to at least one active link.
    ///
    /// The unit owning this gate should produce updates.
    #[default]
    Active,

    /// The gate is not connected to any active links.
    ///
    /// This doesn't necessarily mean that there are no links at all, only
    /// that currently none of the links is interested in receiving updates
    /// from this unit.
    Dormant,

    /// The unit owning this gate should update its configuration.
    ///
    /// The payload contains the new configuration settings that the unit
    /// should adopt, where possible. In particular any changes to upstream
    /// links should be honored as soon as possible.
    Reconfiguring { new_config: Unit },

    /// The unit owning this gate should report its upstream link
    /// configuration.
    ///
    /// The payload should be populated with information about how the unit
    /// owning this gate is linked to its upstream units. This enables the
    /// caller (the Manager) to establish the actual current relationships
    /// between the set of deployed units.
    ReportLinks { report: UpstreamLinkReport },

    /// The unit owning this gate has been triggered by a downstream unit.
    ///
    /// The payload contents have meaning only to the sending and receiving units.
    Triggered { data: TriggerData },
}

impl Eq for GateStatus {}

impl PartialEq for GateStatus {
    fn eq(&self, other: &Self) -> bool {
        // Auto-generated by Rust Analyzer
        core::mem::discriminant(self) == core::mem::discriminant(other)
    }
}

impl Display for GateStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GateStatus::Active => f.write_str("Active"),
            GateStatus::Dormant => f.write_str("Dormant"),
            GateStatus::Reconfiguring { .. } => f.write_str("Reconfiguring"),
            GateStatus::ReportLinks { .. } => f.write_str("ReportLinks"),
            GateStatus::Triggered { .. } => f.write_str("Triggered"),
        }
    }
}

//------------ UnitStatus ----------------------------------------------------

/// The operational status of a unit.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum UnitStatus {
    /// The unit is ready to produce data updates.
    ///
    /// Note that this status does not necessarily mean that the unit is
    /// actually producing updates, only that it could. That is, if a unit’s
    /// gate is dormant and the unit ceases operation because nobody cares,
    /// it is still in healthy status.
    #[default]
    Healthy,

    /// The unit had to temporarily suspend operation.
    ///
    /// If it sets this status, the unit will try to become healthy again
    /// later. The status is typically used if a server has become
    /// unavailable.
    Stalled,

    /// The unit had to permanently suspend operation.
    ///
    /// This status indicates that the unit will not become healthy ever
    /// again. Links to the unit can safely be dropped.
    Gone,
}

impl fmt::Display for UnitStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match *self {
            UnitStatus::Healthy => "healthy",
            UnitStatus::Stalled => "stalled",
            UnitStatus::Gone => "gone",
        })
    }
}

//------------ Terminated ----------------------------------------------------

/// An error signalling that a unit has been terminated.
///
/// In response to this error, a unit’s run function should return.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Terminated;

//------------ GateCommand ---------------------------------------------------

#[derive(Clone, Debug)]
pub enum TriggerData {
    MatchPrefix(Uuid, Prefix, MatchOptions),
}

/// A command sent by a link to a gate.
#[derive(Debug)]
enum GateCommand {
    ReportLinks {
        report: UpstreamLinkReport,
    },

    /// Change the suspension state of a link.
    Suspension {
        /// The slot number of the link to be manipulated.
        slot: Uuid,

        /// Suspend the link?
        suspend: bool,
    },

    /// Subscribe to the gate.
    Subscribe {
        /// Should the subscription start in suspended state?
        suspended: bool,

        /// The sender for the response.
        ///
        /// The response payload is the slot number of the subscription.
        response: oneshot::Sender<SubscribeResponse>,

        direct_update: Option<Weak<dyn AnyDirectUpdate>>,
    },

    Unsubscribe {
        slot: Uuid,
    },

    /// Clone an existing subscription for a Gate worker
    FollowSubscribe {
        slot: Uuid,
        update_sender: UpdateSender,
    },

    FollowUnsubscribe {
        slot: Uuid,
    },

    Reconfigure {
        new_config: Unit,
        new_gate: Gate,
    },

    FollowReconfigure {
        new_config: Unit,
    },

    Trigger {
        data: TriggerData,
    },

    Terminate,

    AttachClone {
        clone_id: Uuid,
        tx: Sender<GateCommand>,
    },

    DetachClone {
        clone_id: Uuid,
    },
}

impl Clone for GateCommand {
    fn clone(&self) -> Self {
        match self {
            Self::FollowSubscribe {
                slot,
                update_sender,
            } => Self::FollowSubscribe {
                slot: *slot,
                update_sender: update_sender.clone(),
            },
            Self::FollowUnsubscribe { slot } => {
                Self::FollowUnsubscribe { slot: *slot }
            }
            Self::FollowReconfigure { new_config } => {
                Self::FollowReconfigure {
                    new_config: new_config.clone(),
                }
            }
            Self::Terminate => Self::Terminate,
            Self::ReportLinks { report } => Self::ReportLinks {
                report: report.clone(),
            },
            _ => panic!("Internal error: Unclonable GateCommand"),
        }
    }
}

impl Display for GateCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GateCommand::ReportLinks { .. } => f.write_str("ReportLinks"),
            GateCommand::Suspension { .. } => f.write_str("Suspension"),
            GateCommand::Subscribe { .. } => f.write_str("Subscribe"),
            GateCommand::Unsubscribe { .. } => f.write_str("Unsubscribe"),
            GateCommand::FollowSubscribe { .. } => {
                f.write_str("FollowSubscribe")
            }
            GateCommand::FollowUnsubscribe { .. } => {
                f.write_str("FollowUnsubscribe")
            }
            GateCommand::Reconfigure { .. } => f.write_str("Reconfigure"),
            GateCommand::FollowReconfigure { .. } => {
                f.write_str("FollowReconfigure")
            }
            GateCommand::Trigger { .. } => f.write_str("Trigger"),
            GateCommand::Terminate => f.write_str("Terminate"),
            GateCommand::AttachClone { .. } => f.write_str("AttachClone"),
            GateCommand::DetachClone { .. } => f.write_str("DetachClone"),
        }
    }
}

//------------ UpdateSender --------------------------------------------------

/// The gate side of sending updates.
#[derive(Clone, Debug)]
struct UpdateSender {
    /// The actual sender.
    ///
    /// This is an option to facilitate deleted dropped links. When sending
    /// fails, we swap this to `None` and then go over the slab again and
    /// drop anything that is `None`. We need to do this because
    /// `Slab::retain` isn’t async but `mpsc::Sender::send` is.
    queue: Option<mpsc::Sender<Result<Update, UnitStatus>>>,

    direct: Option<Weak<dyn AnyDirectUpdate>>,
}

//------------ UpdateReceiver ------------------------------------------------

/// The link side of receiving updates.
type UpdateReceiver = mpsc::Receiver<Result<Update, UnitStatus>>;

//------------ SubscribeResponse ---------------------------------------------

/// The response to a subscribe request.
#[derive(Debug)]
struct SubscribeResponse {
    /// The slot number of this subscription in the gate.
    slot: Uuid,

    /// The update receiver for this subscription.
    receiver: Option<UpdateReceiver>,
}

//------------ Tests ---------------------------------------------------------

/// With the RTRTR design units and targets are connected together by MPSC
/// pipelines each with their own internal queue. What is the performance
/// and resource overhead of these queues vs direct invocation of functions
/// from one component to another? This test is intended to give some insight
/// into these topics.
#[cfg(test)]
mod tests {
    use chrono::SubsecRound;
    use rotonda_store::prefix_record::RouteStatus;
    use smallvec::smallvec;
    use tokio::sync::Notify;

    use crate::{
        payload::Payload, tests::util::internal::{
            enable_logging, get_testable_metrics_snapshot,
        }
    };

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn gate_link_lifecycle_test() {
        use std::str::FromStr;

        use routecore::bgp::{message::PduParseInfo, nlri::afisafi::Ipv4UnicastNlri, path_attributes::OwnedPathAttributes};

        use crate::{payload::{RotondaPaMap, RotondaRoute}};

        // Lifecycle of a connected gate and link:
        //
        //   Client    Unit       Gate    Gate Agent    Link
        //   ───────────────────────────────────────────────
        //     │
        //     │              new()
        //     │╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌▶│╌╌╌╌╌╌╌▶│
        //            (gate, agent) │◀╌╌╌╌╌╌╌│
        //     │◀╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌│        :
        //     │   gate             :        :
        //     │╌╌╌╌╌╌╌▶│           :
        //     │
        //     │  get_gate_status() │
        //     │╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌▶│
        //     │            Dormant │
        //     │◀╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌│
        //     │
        //     │  create_link()
        //     │╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌▶│╌╌╌╌╌╌╌╌╌╌╌▶│
        //     │◀╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌│◀╌╌╌╌╌╌╌╌╌╌╌│
        //     │
        //     │  query()
        //     │╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌▶│╌╌ connect() ╌┐
        //     │                               SUBSCRIBE  │◀╌╌╌╌╌╌╌╌╌╌╌╌╌┘
        //     │                    │◀╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌│
        //     │                    │╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌▶│
        //     │                                          │╌╌╌ recv() ╌╌╌┐
        //     :                                          :              :
        //     :                                          : WAITING...   :
        //     │  get_gate_status() │
        //     │╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌▶│
        //     │            Active  │
        //     │◀╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌│
        //     :
        //     :
        //              │ update_data()
        //              │╌╌╌╌╌╌╌╌╌╌▶│
        //              :           │ . . . . . . . . . . . . . . . . . ▶│
        //                                                               │
        //     │                                   UPDATE │              │
        //     │◀╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌│◀╌╌╌╌╌╌╌╌╌╌╌╌╌┘
        //     :
        //     :
        //
        //                                                │ drop()
        //                                                x
        //
        //     Then some time later either the link goes away which will be
        //     noticed by the gate:
        //
        //     │  get_gate_status() │
        //     │╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌▶│
        //     │            Dormant │
        //     │◀╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌│
        //
        //
        //     Or the gate goes away which will be noticed by the link:
        //
        //                          │ drop()
        //                          x
        //     │  query()
        //     │╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌▶│╌╌ recv() ╌┐
        //     │                    Err(UnitStatus::Gone) │      None │
        //     │◀╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌│◀╌╌╌╌╌╌╌╌╌╌┘
        fn mk_test_payload() -> Payload {
            Payload::new(
                RotondaRoute::Ipv4Unicast(
                    Ipv4UnicastNlri::from_str("1.2.3.0/24").unwrap(),
                    RotondaPaMap::new(
                        OwnedPathAttributes::new(
                            PduParseInfo::modern(),
                            vec![]
                        )
                    )
                ),
                None,
                1234, // IngressId
                RouteStatus::Active,

            )
        }

        eprintln!("STARTING");
        // Create a gate. Updates sent via the gate will be received by links.
        // the minimum allowed queue capacity is 1
        let (gate, mut agent) = Gate::new(1);

        // Create a link from the gate agent to receive updates from the gate.
        let mut link = agent.create_link();

        #[derive(Debug)]
        struct TestDirectUpdateTarget(Arc<Notify>);

        #[async_trait]
        impl DirectUpdate for TestDirectUpdateTarget {
            async fn direct_update(&self, update: Update) {
                assert!(matches!(update, Update::Bulk(_)));
                if let Update::Bulk(payload) = update {
                    assert_eq!(payload.len(), 2);
                    assert_eq!(payload[0], mk_test_payload());
                    assert_eq!(payload[1], mk_test_payload());
                    self.0.notify_one();
                }
            }
        }

        impl AnyDirectUpdate for TestDirectUpdateTarget {}

        let notify = Arc::new(Notify::default());
        let test_target = Arc::new(TestDirectUpdateTarget(notify.clone()));

        eprintln!("SETTING LINK TO DIRECT UPDATE MODE");
        link.set_direct_update_target(test_target.clone());

        let gate = Arc::new(gate);
        let gate_clone = gate.clone();

        // "Run" the gate like a unit does.
        tokio::spawn(async move {
            loop {
                gate.process().await.unwrap();
            }
        });

        let metrics = get_testable_metrics_snapshot(&gate_clone.metrics());
        assert_eq!(metrics.with_name::<usize>("num_updates"), 0);
        assert_eq!(metrics.with_name::<String>("last_update"), "N/A");
        assert_eq!(metrics.with_name::<String>("since_last_update"), "-1");

        eprintln!(
            "TESTING THAT UPDATES ARE DROPPED WHEN THERE IS NO DOWNSTREAM"
        );

        // Build an update to send
        let update = Update::Single(mk_test_payload());
        gate_clone.update_data(update).await;

        let metrics = get_testable_metrics_snapshot(&gate_clone.metrics());
        assert_eq!(metrics.with_name::<usize>("num_updates"), 1);
        assert_eq!(metrics.with_name::<usize>("num_dropped_updates"), 1);
        assert_eq!(
            metrics
                .with_name::<DateTime<Utc>>("last_update")
                .round_subsecs(0),
            Utc::now().round_subsecs(0)
        );
        assert!(metrics.with_name::<i64>("since_last_update") >= 0);

        eprintln!("CONNECTING LINK TO GATE");
        link.connect(false).await.unwrap();

        // Build an update to send
        let update =
            Update::Bulk(smallvec![mk_test_payload(), mk_test_payload()]);

        // Send the update through the gate
        eprintln!("SENDING PAYLOAD");
        gate_clone.update_data(update).await;

        eprintln!("WAITING FOR PAYLOAD TO BE RECEIVED BY THE TEST TARGET");
        let timeout = Box::pin(tokio::time::sleep(Duration::from_secs(3)));
        let notified = Box::pin(notify.notified());
        assert!(matches!(select(timeout, notified).await, Either::Right(..)));

        let metrics = get_testable_metrics_snapshot(&gate_clone.metrics());
        assert_eq!(metrics.with_name::<usize>("num_updates"), 2);
        assert_eq!(metrics.with_name::<usize>("num_dropped_updates"), 1);
        assert_eq!(
            metrics
                .with_name::<DateTime<Utc>>("last_update")
                .round_subsecs(0),
            Utc::now().round_subsecs(0)
        );
        let since_last_update = metrics.with_name::<i64>("since_last_update");
        assert_eq!(metrics.with_name::<usize>("update_set_size"), 2);

        eprintln!("WAITING TO CHECK THAT SINCE_LAST_UPDATE METRIC UPDATES");
        tokio::time::sleep(Duration::from_secs(2)).await;
        let metrics = get_testable_metrics_snapshot(&gate_clone.metrics());
        let new_since_last_update =
            metrics.with_name::<i64>("since_last_update");

        assert!(new_since_last_update > since_last_update);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn gate_clones_terminate_when_parent_gate_is_dropped() {
        let (gate, agent) = Gate::new(10);
        let gate_clone = gate.clone();

        eprintln!("CHECKING GATE DOES NOT YET HAVE CLONE SENDER");
        assert!(matches!(&gate.state, GateState::Normal(NormalGateState {
                clone_senders, .. }) if clone_senders.is_empty()));

        // Give the parent gate a chance to process the AttachClone command
        // that will be sent by the new clone.
        let _ = gate
            .process_until(tokio::time::sleep(Duration::from_secs(1)))
            .await;

        eprintln!("CHECKING GATE HAS CLONE SENDER");
        assert!(matches!(&gate.state, GateState::Normal(NormalGateState {
                clone_senders, .. }) if !clone_senders.is_empty()));

        eprintln!("SENDING TERMINATION COMMAND");
        agent.terminate().await;

        eprintln!("CHECKING GATE IS TERMINATED");
        assert_eq!(gate.process().await, Err(Terminated));

        // Typically at this point the unit owning the gate will exit its
        // run() function and by doing so drop the master instance of the
        // gate from which the clones were made.
        drop(gate);

        // Next time a cloned gate tries to check for commands it finds
        // that the sender of the commands has been dropped because it was
        // owned by the "parent" gate and so the clone gate exits the
        // process() function with Err(Terminated).
        eprintln!("CHECKING GATE CLONE IS TERMINATED");
        assert_eq!(gate_clone.process().await, Err(Terminated));

        eprintln!("GATE AND GATE CLONE ARE TERMINATED");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn gate_clones_receive_termination_signal() {
        enable_logging("trace");
        let (gate, agent) = Gate::new(10);
        let gate_clone = gate.clone();

        eprintln!("CHECKING GATE DOES NOT YET HAVE CLONE SENDER");
        assert!(matches!(&gate.state, GateState::Normal(NormalGateState {
                clone_senders, .. }) if clone_senders.is_empty()));

        // Give the parent gate a chance to process the AttachClone command
        // that will be sent by the new clone.
        let _ = gate
            .process_until(tokio::time::sleep(Duration::from_secs(1)))
            .await;

        eprintln!("CHECKING GATE HAS CLONE SENDER");
        assert!(matches!(&gate.state, GateState::Normal(NormalGateState { 
                clone_senders, .. }) if !clone_senders.is_empty()));

        eprintln!("SENDING TERMINATION COMMAND");
        agent.terminate().await;

        eprintln!("CHECKING GATE IS TERMINATED");
        assert_eq!(gate.process().await, Err(Terminated));

        eprintln!("CHECKING GATE CLONE IS TERMINATED");
        assert_eq!(gate_clone.process().await, Err(Terminated));

        eprintln!("GATE AND GATE CLONE ARE TERMINATED");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn gate_parent_cleans_up_when_clone_terminates() {
        let (gate, _agent) = Gate::new(10);
        let gate_clone = gate.clone();

        eprintln!("CHECKING GATE DOES NOT YET HAVE CLONE SENDER");
        assert!(matches!(&gate.state, GateState::Normal(NormalGateState { 
                clone_senders, .. }) if clone_senders.is_empty()));

        // Give the parent gate a chance to process the AttachClone command
        // that will be sent by the new clone.
        let _ = gate
            .process_until(tokio::time::sleep(Duration::from_secs(1)))
            .await;

        eprintln!("CHECKING GATE HAS CLONE SENDER");
        assert!(matches!(&gate.state, GateState::Normal(NormalGateState { 
                clone_senders, .. }) if !clone_senders.is_empty()));

        eprintln!("DROPPING CLONED GATE");
        drop(gate_clone);

        // Allow the DetachClone command to be received and acted upon
        eprintln!("PROCESS COMMANDS IN PARENT GATE");
        gate.wait(1).await.unwrap();

        eprintln!("CHECKING GATE HAS NO CLONE SENDER");
        assert!(matches!(&gate.state, GateState::Normal(NormalGateState { 
                clone_senders, .. }) if clone_senders.is_empty()));
    }
}
