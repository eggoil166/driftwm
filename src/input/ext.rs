use std::io::{self, BufReader, BufRead};
use std::net::TcpStream;
use serde::{Deserialize, Serialize};
use serde::de::{self, Deserializer}; 
use smithay::reexports::calloop::{
    channel::{self, Event as ChannelEvent, Sender},
    LoopHandle,
};
use std::time::{Duration, Instant};
use std::error::Error;
use std::thread;
use crate::state::DriftWm;

#[derive(Serialize, Deserialize, Debug)]
pub struct NormalizedCoords {
    pub x: f64,
    pub y: f64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Angles {
    pub yaw: f64,
    pub pitch: f64,
}

#[derive(Serialize, Debug)]
pub struct CamControlEvent {
    t: f64,
    detected: bool,
    raw: Option<Angles>,
    filtered: Option<Angles>,
    normcoords: Option<NormalizedCoords>,
}

impl<'de> Deserialize<'de> for CamControlEvent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where 
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct EventHelper {
            t: f64,
            detected: bool,
            #[serde(default)]
            raw: Option<Angles>,
            #[serde(default)]
            filtered: Option<Angles>,
            #[serde(default, rename = "screen")]
            normcoords: Option<NormalizedCoords>,
        }

        let event = EventHelper::deserialize(deserializer)?;
        if event.detected {
            if event.normcoords.is_none() {
                return Err(de::Error::missing_field("normcoords"));
            }
        }

        Ok(CamControlEvent {
            t: event.t,
            detected: event.detected,
            raw: event.raw,
            filtered: event.filtered,
            normcoords: event.normcoords,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum ExternalInputMsg {
    HeadJoystick {
        x: f64,
        y: f64,
        cam_t: f64,
        now: Instant,
    },
    LostTracking {
        now: Instant,
    },
}

impl CamControlEvent {
    fn into_external_msg(self, now: Instant) -> ExternalInputMsg {
        if self.detected {
            match self.normcoords {
                Some(normcoords) => ExternalInputMsg::HeadJoystick {
                    x: normcoords.x,
                    y: normcoords.y,
                    cam_t: self.t,
                    now,
                },
                None => ExternalInputMsg::LostTracking {
                    now,
                },
            }
        } else {
            ExternalInputMsg::LostTracking {
                now,
            }
        }
    }
}

fn consume_events(stream: TcpStream, tx: &Sender<ExternalInputMsg>) -> io::Result<()> {
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    loop {
        line.clear();
        let bytes_read = reader.read_line(&mut line)?;
        if bytes_read == 0 {
            return Ok(());
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let event: CamControlEvent = match serde_json::from_str(trimmed) {
            Ok(event) => event,
            Err(err) => {
                tracing::warn!("failed to parse camcontrol event: {err}; line={trimmed:?}");
                continue;
            }
        };
        let now = Instant::now();
        let msg = event.into_external_msg(now);
        if tx.send(msg).is_err() {
            tracing::warn!("external input receiver dropped");
            break;
        }
    }
    Ok(())
}

fn camcontrol_reader_thread(
    host: String,
    port: u16,
    tx: Sender<ExternalInputMsg>,
) {
    let addr = format!("{host}:{port}");
    let mut backoff = Duration::from_millis(250);
    let max_backoff = Duration::from_secs(5);
    loop {
        tracing::info!("attempting to connect to camcontrol at address {addr}");
        match TcpStream::connect(&addr) {
            Ok(stream) => {
                tracing::info!("connected to camcontnrol at {addr}");
                backoff = Duration::from_millis(250);
                if let Err(err) = consume_events(stream, &tx) {
                    tracing::warn!("camcontrol stream error: {err}");
                }
                let _ = tx.send(ExternalInputMsg::LostTracking {
                    now: Instant::now(),
                });
                tracing::warn!("camcontrol disconnected, reconnecting");
            }
            Err(err) => {
                tracing::warn!("failed to connect to camcontrol at {addr}: {err}");
            }
        }
        thread::sleep(backoff);
        backoff = (backoff * 2).min(max_backoff)
    }
}

pub(crate) fn register_external_input(
    handle: &LoopHandle<'static, DriftWm>,
    host: String,
    port: u16,
) -> Result<(), Box<dyn Error>> {
    let (tx, rx) = channel::channel::<ExternalInputMsg>();
    thread::Builder::new()
        .name("camcontrol-external-input".to_string())
        .spawn(move || {
            camcontrol_reader_thread(host, port, tx);
        })?;
    handle.insert_source(rx, |event, _metadata, state| {
        match event {
            ChannelEvent::Msg(msg) => {
                state.handle_external_input(msg);
            }
            ChannelEvent::Closed => {
                tracing::warn!("external input channel closed");
            }
        }
    })?;

    Ok(())
}

impl DriftWm {
    pub(crate) fn handle_external_input(&mut self, msg: ExternalInputMsg) {
        tracing::debug!("external input: {msg:?}");
    }
}