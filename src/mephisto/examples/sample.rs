// Copyright 2023 tison <wander4096@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use mephisto::{
    acceptor::Acceptor,
    env::Env,
    leader::Leader,
    message::{PaxosMessage, RequestMessage},
    replica::Replica,
    Command, Config, ConfigCommand, OperationCommand, ProcessId, WINDOW,
};
use signal_hook::{
    consts::{SIGINT, SIGTERM},
    iterator::Signals,
};
use tracing::info;

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().init();

    const NACCEPTORS: u64 = 3;
    const NREPLICAS: u64 = 2;
    const NLEADERS: u64 = 2;
    const NREQUESTS: u64 = 10;
    const NCONFIGS: u64 = 3;

    let env = Arc::new(Mutex::new(Env::default()));
    let mut initial_config = Config::default();
    let c = 0;
    let mut req_id = 0;

    for i in 0..NREPLICAS {
        let pid = ProcessId::new(format!("replica: {i}"));
        initial_config.replicas.push(pid);
    }
    for i in 0..NACCEPTORS {
        let pid = ProcessId::new(format!("acceptor: {c}.{i}"));
        initial_config.acceptors.push(pid);
    }
    for i in 0..NLEADERS {
        let pid = ProcessId::new(format!("leader: {c}.{i}"));
        initial_config.leaders.push(pid);
    }

    for r in initial_config.replicas.iter().cloned() {
        Replica::start(env.clone(), r, initial_config.clone());
    }
    for a in initial_config.acceptors.iter().cloned() {
        Acceptor::start(env.clone(), a);
    }
    for l in initial_config.leaders.iter().cloned() {
        Leader::start(env.clone(), l, initial_config.clone());
    }

    for i in 0..NREQUESTS {
        let pid = ProcessId::new(format!("client: {c}.{i}"));
        for r in initial_config.replicas.iter().cloned() {
            {
                let env = env.lock().unwrap();
                env.send_msg(
                    r,
                    PaxosMessage::RequestMessage(RequestMessage {
                        src: pid.clone(),
                        command: Command::Operation(OperationCommand {
                            client: pid.clone(),
                            req_id: {
                                req_id += 1;
                                req_id - 1
                            },
                            op: format!("operation {c}.{i}").into_bytes(),
                        }),
                    }),
                );
            }
            thread::sleep(Duration::from_secs(1));
        }
    }

    for c in 1..NCONFIGS {
        // Create new configuration
        let mut config = Config {
            replicas: initial_config.replicas.clone(),
            ..Default::default()
        };
        for i in 0..NACCEPTORS {
            let pid = ProcessId::new(format!("acceptor: {c}.{i}"));
            config.acceptors.push(pid);
        }
        for i in 0..NLEADERS {
            let pid = ProcessId::new(format!("leader: {c}.{i}"));
            config.leaders.push(pid);
        }
        for a in config.acceptors.iter().cloned() {
            Acceptor::start(env.clone(), a);
        }
        for l in config.leaders.iter().cloned() {
            Leader::start(env.clone(), l, config.clone());
        }

        // Send reconfiguration request
        for r in config.replicas.iter().cloned() {
            let pid = ProcessId::new(format!("master {c}.{r}"));
            {
                let env = env.lock().unwrap();
                env.send_msg(
                    r,
                    PaxosMessage::RequestMessage(RequestMessage {
                        src: pid.clone(),
                        command: Command::Config(ConfigCommand {
                            client: pid,
                            req_id: {
                                req_id += 1;
                                req_id - 1
                            },
                            config: config.clone(),
                        }),
                    }),
                );
            }
            thread::sleep(Duration::from_secs(1));
        }
        for i in 0..WINDOW - 1 {
            let pid = ProcessId::new(format!("master {c}.{i}"));
            for r in config.replicas.iter().cloned() {
                {
                    let env = env.lock().unwrap();
                    env.send_msg(
                        r,
                        PaxosMessage::RequestMessage(RequestMessage {
                            src: pid.clone(),
                            command: Command::Operation(OperationCommand {
                                client: pid.clone(),
                                req_id: {
                                    req_id += 1;
                                    req_id - 1
                                },
                                op: "operation noop".to_owned().into_bytes(),
                            }),
                        }),
                    );
                }
                thread::sleep(Duration::from_secs(1));
            }
        }
        for i in 0..NREQUESTS {
            let pid = ProcessId::new(format!("client: {c}.{i}"));
            for r in initial_config.replicas.iter().cloned() {
                {
                    let env = env.lock().unwrap();
                    env.send_msg(
                        r,
                        PaxosMessage::RequestMessage(RequestMessage {
                            src: pid.clone(),
                            command: Command::Operation(OperationCommand {
                                client: pid.clone(),
                                req_id: {
                                    req_id += 1;
                                    req_id - 1
                                },
                                op: format!("operation {c}.{i}").into_bytes(),
                            }),
                        }),
                    );
                }
                thread::sleep(Duration::from_secs(1));
            }
        }
    }

    let mut signal = Signals::new([SIGINT, SIGTERM])?;
    if let Some(sig) = signal.forever().next() {
        info!("Received signal {sig}.");
    }
    Ok(())
}
