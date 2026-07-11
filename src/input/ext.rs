use std::error::Error;
use std::io::{self, BufRead, BufReader};
use std::net::TcpStream;
use std::thread;
use std::time::{Duration, Instant};

use serde::Deserialize;
use serde::de::{self, Deserializer};
use smithay::reexports::calloop::{
    LoopHandle,
    channel::{self, Event as ChannelEvent, Sender},
};
use smithay::utils::{Logical, Point};

use crate::state::DriftWm;

#[derive(Deserialize, Debug)]
struct NormalizedCoords {
    x: f64,
    y: f64,
}

#[derive(Debug)]
struct CamControlEvent {
    detected: bool,
    screen: Option<NormalizedCoords>,
}

impl<'de> Deserialize<'de> for CamControlEvent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct EventHelper {
            detected: bool,
            #[serde(default, rename = "screen")]
            screen: Option<NormalizedCoords>,
        }

        let event = EventHelper::deserialize(deserializer)?;
        if event.detected && event.screen.is_none() {
            return Err(de::Error::missing_field("screen"));
        }

        Ok(CamControlEvent {
            detected: event.detected,
            screen: event.screen,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum ExternalInputMsg {
    HeadJoystick { x: f64, y: f64, now: Instant },
    LostTracking,
}

impl CamControlEvent {
    fn into_external_msg(self, now: Instant) -> ExternalInputMsg {
        if self.detected {
            match self.screen {
                Some(screen) => ExternalInputMsg::HeadJoystick {
                    x: screen.x,
                    y: screen.y,
                    now,
                },
                None => ExternalInputMsg::LostTracking,
            }
        } else {
            ExternalInputMsg::LostTracking
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

fn camcontrol_reader_thread(host: String, port: u16, tx: Sender<ExternalInputMsg>) {
    let addr = format!("{host}:{port}");
    let mut backoff = Duration::from_millis(250);
    let max_backoff = Duration::from_secs(5);
    loop {
        tracing::debug!("attempting to connect to camcontrol at address {addr}");
        match TcpStream::connect(&addr) {
            Ok(stream) => {
                tracing::info!("connected to camcontrol at {addr}");
                backoff = Duration::from_millis(250);
                if let Err(err) = consume_events(stream, &tx) {
                    tracing::warn!("camcontrol stream error: {err}");
                }
                let _ = tx.send(ExternalInputMsg::LostTracking);
                tracing::info!("camcontrol disconnected, reconnecting");
            }
            Err(err) => {
                tracing::debug!("failed to connect to camcontrol at {addr}: {err}");
            }
        }
        thread::sleep(backoff);
        backoff = (backoff * 2).min(max_backoff);
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
    handle.insert_source(rx, |event, _metadata, state| match event {
        ChannelEvent::Msg(msg) => {
            state.handle_external_input(msg);
        }
        ChannelEvent::Closed => {
            tracing::warn!("external input channel closed");
        }
    })?;

    Ok(())
}

fn joystick_vector(x: f64, y: f64) -> (f64, f64) {
    if !x.is_finite() || !y.is_finite() {
        return (0.0, 0.0);
    }

    let dx = ((x - 0.5) * 2.0).clamp(-1.0, 1.0);
    let dy = ((y - 0.5) * 2.0).clamp(-1.0, 1.0);
    let mag = dx.hypot(dy);

    // Radial deadzone keeps small head jitter from moving the viewport.
    const DEADZONE: f64 = 0.42;
    if mag <= DEADZONE {
        return (0.0, 0.0);
    }

    let dir_x = dx / mag;
    let dir_y = dy / mag;
    let normalized = ((mag.min(1.0) - DEADZONE) / (1.0 - DEADZONE)).clamp(0.0, 1.0);

    const EXP_CURVE: f64 = 1.25;
    let curved = normalized.powf(EXP_CURVE);
    (dir_x * curved, dir_y * curved)
}

impl DriftWm {
    pub(crate) fn handle_external_input(&mut self, msg: ExternalInputMsg) {
        let input_blocked = !matches!(self.session_lock, crate::state::SessionLock::Unlocked)
            || self.is_fullscreen();
        if input_blocked {
            self.ext_last_seen = None;
            return;
        }

        match msg {
            ExternalInputMsg::LostTracking => {
                self.ext_last_seen = None;
                self.with_output_state(|os| {
                    os.momentum.stop();
                });
                return;
            }

            ExternalInputMsg::HeadJoystick { x, y, now } => {
                let last_seen = match self.ext_last_seen {
                    Some(last_seen) => last_seen,
                    None => {
                        self.ext_last_seen = Some(now);
                        return;
                    }
                };

                self.ext_last_seen = Some(now);
                let mut dt = now.duration_since(last_seen).as_secs_f64();

                if !dt.is_finite() || dt <= 0.0 {
                    return;
                }

                dt = dt.min(0.05);
                let (jx, jy) = joystick_vector(x, y);

                if jx == 0.0 && jy == 0.0 {
                    self.with_output_state(|os| {
                        os.momentum.stop();
                    });
                    return;
                }

                const PPS: f64 = 1200.0;
                let dx_px = jx * PPS * dt;
                let dy_px = jy * PPS * dt;
                self.pan_cam(dx_px, dy_px);
            }
        }
    }

    fn pan_cam(&mut self, dx_px: f64, dy_px: f64) {
        if !dx_px.is_finite() || !dy_px.is_finite() {
            return;
        }

        self.idle_notifier_state.notify_activity(&self.seat);
        self.wake_dpms_off_outputs();

        let Some(canvas_delta) = self.with_output_state(|os| {
            os.camera_target = None;
            os.zoom_target = None;
            os.zoom_animation_center = None;
            os.overview_return = None;
            os.momentum.stop();
            let canvas_delta: Point<f64, Logical> = Point::from((dx_px / os.zoom, dy_px / os.zoom));
            os.camera.x += canvas_delta.x;
            os.camera.y += canvas_delta.y;
            canvas_delta
        }) else {
            return;
        };

        self.update_output_from_camera();
        let pos = self.seat.get_pointer().unwrap().current_location();
        self.warp_pointer(pos + canvas_delta);
        self.mark_all_dirty();
    }
}
