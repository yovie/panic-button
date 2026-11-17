use anyhow::{Context as _, bail};
use chrono::Utc;
use panic_button::mqtt::parse_broker_url;
use rumqttc::{AsyncClient, MqttOptions, QoS};
use serde_json::{Value, json};
use std::{
    sync::atomic::{AtomicU32, Ordering},
    time::{Duration, Instant},
};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::mpsc;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

static MESSAGE_ID: AtomicU32 = AtomicU32::new(1);

const ROUTE: &[(f64, f64)] = &[
    (-6.1944, 106.8229),
    (-6.1754, 106.8272),
    (-6.1750, 106.8450),
    (-6.1900, 106.8500),
    (-6.2050, 106.8450),
    (-6.2100, 106.8300),
    (-6.1944, 106.8229),
];

const NAMES: &[(&str, &str)] = &[
    ("Sim Alpha", "SA"),
    ("Sim Bravo", "SB"),
    ("Sim Charlie", "SC"),
    ("Sim Delta", "SD"),
    ("Sim Echo", "SE"),
    ("Sim Foxtrot", "SF"),
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Http,
    Mqtt,
}

struct Args {
    mode: Mode,
    server: String,
    mqtt: String,
    devices: usize,
    interval: f64,
    region: String,
    channel: String,
    auto_panic: u64,
}

impl Args {
    fn parse() -> anyhow::Result<Self> {
        let mut args = Self {
            mode: Mode::Http,
            server: "http://localhost:8080".to_string(),
            mqtt: "mqtt://localhost:1883".to_string(),
            devices: 3,
            interval: 2.0,
            region: "ID".to_string(),
            channel: "Panic".to_string(),
            auto_panic: 0,
        };

        let mut iter = std::env::args().skip(1);
        while let Some(argument) = iter.next() {
            match argument.as_str() {
                "--mode" => {
                    let mode = iter.next().context("--mode needs a value")?;
                    args.mode = match mode.as_str() {
                        "http" => Mode::Http,
                        "mqtt" => Mode::Mqtt,
                        other => bail!("unknown mode {other}, use http or mqtt"),
                    };
                }
                "--server" => args.server = iter.next().context("--server needs a value")?,
                "--mqtt" => args.mqtt = iter.next().context("--mqtt needs a value")?,
                "--devices" => {
                    args.devices = iter.next().context("--devices needs a value")?.parse()?
                }
                "--interval" => {
                    args.interval = iter.next().context("--interval needs a value")?.parse()?
                }
                "--region" => args.region = iter.next().context("--region needs a value")?,
                "--channel" => args.channel = iter.next().context("--channel needs a value")?,
                "--auto-panic" => {
                    args.auto_panic = iter.next().context("--auto-panic needs a value")?.parse()?
                }
                "--help" | "-h" => {
                    print_help();
                    std::process::exit(0);
                }
                other => bail!("unknown argument {other}, try --help"),
            }
        }

        args.devices = args.devices.clamp(1, NAMES.len());
        args.interval = args.interval.max(0.2);
        Ok(args)
    }
}

fn print_help() {
    println!(
        "panic-button simulator\n\
         \n\
         Options:\n\
         --mode <http|mqtt>   Delivery mode (default: http)\n\
         --server <url>       Ingest base URL for http mode (default: http://localhost:8080)\n\
         --mqtt <url>         MQTT broker URL for mqtt mode (default: mqtt://localhost:1883)\n\
         --devices <1-6>      Number of simulated devices (default: 3)\n\
         --interval <secs>    Position update interval (default: 2.0)\n\
         --region <region>    Meshtastic region for mqtt topics (default: ID)\n\
         --channel <name>     Meshtastic channel for mqtt topics (default: Panic)\n\
         --auto-panic <secs>  Demo mode: toggle a panic every N seconds (default: off)\n\
         \n\
         Interactive keys (press Enter after the key):\n\
         p = panic a device, r = resolve all panics, q = quit"
    );
}

enum Command {
    Panic,
    Resolve,
    Quit,
}

struct SimDevice {
    index: usize,
    node_number: u32,
    name: String,
    short_name: String,
    distance_m: f64,
    speed_base_kmh: f64,
    battery: f64,
    panic: bool,
    lat: f64,
    lon: f64,
    heading: f64,
    speed_kmh: f64,
}

impl SimDevice {
    fn new(index: usize, distance_m: f64) -> Self {
        let (name, short_name) = NAMES[index];
        Self {
            index,
            node_number: 0x0000_c001 + index as u32,
            name: name.to_string(),
            short_name: short_name.to_string(),
            distance_m,
            speed_base_kmh: 40.0 + index as f64 * 8.0,
            battery: 90.0 - index as f64 * 5.0,
            panic: false,
            lat: 0.0,
            lon: 0.0,
            heading: 0.0,
            speed_kmh: 0.0,
        }
    }

    fn id(&self) -> String {
        format!("!{:08x}", self.node_number)
    }
}

struct Route {
    points: &'static [(f64, f64)],
    segments: Vec<f64>,
    total: f64,
}

impl Route {
    fn new(points: &'static [(f64, f64)]) -> Self {
        let segments: Vec<f64> = points
            .windows(2)
            .map(|pair| haversine_m(pair[0], pair[1]))
            .collect();
        let total = segments.iter().sum();
        Self {
            points,
            segments,
            total,
        }
    }

    fn position_at(&self, distance: f64) -> (f64, f64, f64) {
        let mut remaining = distance.rem_euclid(self.total);
        for (index, segment) in self.segments.iter().enumerate() {
            if remaining <= *segment || index == self.segments.len() - 1 {
                let (lat1, lon1) = self.points[index];
                let (lat2, lon2) = self.points[index + 1];
                let ratio = if *segment > 0.0 {
                    (remaining / segment).clamp(0.0, 1.0)
                } else {
                    0.0
                };
                let lat = lat1 + (lat2 - lat1) * ratio;
                let lon = lon1 + (lon2 - lon1) * ratio;
                return (lat, lon, bearing(lat1, lon1, lat2, lon2));
            }
            remaining -= segment;
        }

        let (lat, lon) = self.points[0];
        (lat, lon, 0.0)
    }
}

struct Context<'a> {
    args: &'a Args,
    http: Option<&'a reqwest::Client>,
    mqtt: Option<&'a AsyncClient>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse()?;
    let route = Route::new(ROUTE);

    let http = match args.mode {
        Mode::Http => Some(reqwest::Client::new()),
        Mode::Mqtt => None,
    };

    let mqtt = match args.mode {
        Mode::Http => None,
        Mode::Mqtt => {
            let broker = parse_broker_url(&args.mqtt)?;
            let mut options = MqttOptions::new(
                format!("panic-button-sim-{}", std::process::id()),
                broker.host,
                broker.port,
            );
            options.set_keep_alive(Duration::from_secs(30));
            if let Some(username) = broker.username {
                options.set_credentials(username, broker.password.unwrap_or_default());
            }
            let (client, mut eventloop) = AsyncClient::new(options, 256);
            tokio::spawn(async move {
                loop {
                    if let Err(error) = eventloop.poll().await {
                        warn!(%error, "MQTT event loop error, retrying");
                        tokio::time::sleep(Duration::from_secs(3)).await;
                    }
                }
            });
            Some(client)
        }
    };

    let context = Context {
        args: &args,
        http: http.as_ref(),
        mqtt: mqtt.as_ref(),
    };

    let mut devices: Vec<SimDevice> = (0..args.devices)
        .map(|index| {
            let mut device = SimDevice::new(index, index as f64 * 700.0);
            let (lat, lon, heading) = route.position_at(device.distance_m);
            device.lat = lat + index as f64 * 0.0015;
            device.lon = lon + index as f64 * 0.0015;
            device.heading = heading;
            device.speed_kmh = device.speed_base_kmh;
            device
        })
        .collect();

    info!(
        mode = ?args.mode,
        devices = devices.len(),
        interval = args.interval,
        "simulator started (keys: p = panic, r = resolve, q = quit)"
    );

    if args.mode == Mode::Mqtt {
        for device in &devices {
            publish_nodeinfo(&context, device).await;
            publish_position(&context, device).await;
        }
    }

    let (command_tx, mut command_rx) = mpsc::channel::<Command>(16);
    tokio::spawn(async move {
        let mut lines = BufReader::new(tokio::io::stdin()).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            let command = match line.trim() {
                "p" | "P" => Command::Panic,
                "r" | "R" => Command::Resolve,
                "q" | "Q" => Command::Quit,
                _ => continue,
            };
            if command_tx.send(command).await.is_err() {
                break;
            }
        }
    });

    let mut ticker = tokio::time::interval(Duration::from_secs_f64(args.interval));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut auto_panic_timer = Instant::now();
    let mut cursor = 0usize;
    let mut tick: u64 = 0;

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                tick += 1;
                advance_devices(&route, &mut devices, args.interval);

                for device in &devices {
                    match args.mode {
                        Mode::Http => send_ingest(&context, device, "heartbeat").await,
                        Mode::Mqtt => {
                            publish_position(&context, device).await;
                            if tick.is_multiple_of(5) {
                                publish_telemetry(&context, device, tick).await;
                            }
                            if tick.is_multiple_of(30) {
                                publish_nodeinfo(&context, device).await;
                            }
                        }
                    }
                }

                if args.auto_panic > 0
                    && auto_panic_timer.elapsed() >= Duration::from_secs(args.auto_panic)
                {
                    auto_panic_timer = Instant::now();
                    if devices.iter().any(|device| device.panic) {
                        info!("auto demo: resolving all panic alerts");
                        resolve_all(&context, &mut devices).await;
                    } else {
                        trigger_panic(&context, &mut devices, &mut cursor).await;
                    }
                }
            }
            Some(command) = command_rx.recv() => match command {
                Command::Panic => trigger_panic(&context, &mut devices, &mut cursor).await,
                Command::Resolve => resolve_all(&context, &mut devices).await,
                Command::Quit => break,
            },
        }
    }

    Ok(())
}

fn advance_devices(route: &Route, devices: &mut [SimDevice], interval: f64) {
    for device in devices {
        let wave = ((device.distance_m / 400.0) + device.index as f64).sin() * 3.0;
        device.speed_kmh = (device.speed_base_kmh + wave).max(5.0);
        device.distance_m += device.speed_kmh / 3.6 * interval;

        let (lat, lon, heading) = route.position_at(device.distance_m);
        device.lat = lat + device.index as f64 * 0.0015;
        device.lon = lon + device.index as f64 * 0.0015;
        device.heading = heading;

        device.battery -= 0.02;
        if device.battery < 20.0 {
            device.battery = 95.0;
        }
    }
}

async fn trigger_panic(context: &Context<'_>, devices: &mut [SimDevice], cursor: &mut usize) {
    for _ in 0..devices.len() {
        let index = *cursor % devices.len();
        *cursor = cursor.wrapping_add(1);

        if devices[index].panic {
            continue;
        }

        devices[index].panic = true;
        let device = &devices[index];
        info!(device = %device.name, id = %device.id(), "PANIC triggered");

        match context.args.mode {
            Mode::Http => send_ingest(context, device, "panic").await,
            Mode::Mqtt => publish_detection(context, device, true).await,
        }
        return;
    }

    warn!("all simulated devices are already in panic");
}

async fn resolve_all(context: &Context<'_>, devices: &mut [SimDevice]) {
    for device in devices.iter_mut().filter(|device| device.panic) {
        device.panic = false;
        info!(device = %device.name, id = %device.id(), "panic resolved");
        match context.args.mode {
            Mode::Http => send_ingest(context, device, "panic_clear").await,
            Mode::Mqtt => publish_detection(context, device, false).await,
        }
    }
}

async fn send_ingest(context: &Context<'_>, device: &SimDevice, event: &str) {
    let Some(client) = context.http else {
        return;
    };

    let payload = json!({
        "device_id": device.id(),
        "name": device.name,
        "lat": device.lat,
        "lon": device.lon,
        "speed_kmh": device.speed_kmh,
        "heading": device.heading,
        "battery": device.battery.round(),
        "protocol": "simulator",
        "event": event,
    });

    let url = format!("{}/api/ingest", context.args.server.trim_end_matches('/'));
    if let Err(error) = client.post(&url).json(&payload).send().await {
        warn!(%error, %url, "failed to send ingest event");
    }
}

fn topic_for(context: &Context<'_>, device: &SimDevice) -> String {
    format!(
        "msh/{}/2/json/{}/{}",
        context.args.region,
        context.args.channel,
        device.id()
    )
}

async fn publish_nodeinfo(context: &Context<'_>, device: &SimDevice) {
    let Some(client) = context.mqtt else {
        return;
    };

    let payload = json!({
        "id": next_message_id(),
        "channel": 0,
        "from": device.node_number,
        "payload": {
            "id": device.id(),
            "longname": device.name,
            "shortname": device.short_name,
            "hardware": 0,
            "role": "CLIENT",
        },
        "sender": device.id(),
        "timestamp": Utc::now().timestamp(),
        "to": -1,
        "type": "nodeinfo",
    });
    publish(client, &topic_for(context, device), payload).await;
}

async fn publish_position(context: &Context<'_>, device: &SimDevice) {
    let Some(client) = context.mqtt else {
        return;
    };

    let payload = json!({
        "id": next_message_id(),
        "channel": 0,
        "from": device.node_number,
        "payload": {
            "latitude_i": (device.lat * 1e7).round() as i64,
            "longitude_i": (device.lon * 1e7).round() as i64,
            "altitude": 12,
            "time": Utc::now().timestamp(),
            "precision_bits": 32,
            "ground_speed": device.speed_kmh / 3.6,
            "ground_track": device.heading,
        },
        "sender": device.id(),
        "timestamp": Utc::now().timestamp(),
        "to": -1,
        "type": "position",
    });
    publish(client, &topic_for(context, device), payload).await;
}

async fn publish_telemetry(context: &Context<'_>, device: &SimDevice, tick: u64) {
    let Some(client) = context.mqtt else {
        return;
    };

    let payload = json!({
        "id": next_message_id(),
        "channel": 0,
        "from": device.node_number,
        "payload": {
            "battery_level": device.battery.round() as u8,
            "voltage": 3.9,
            "uptime_seconds": (tick as f64 * context.args.interval) as u64,
        },
        "sender": device.id(),
        "timestamp": Utc::now().timestamp(),
        "to": -1,
        "type": "telemetry",
    });
    publish(client, &topic_for(context, device), payload).await;
}

async fn publish_detection(context: &Context<'_>, device: &SimDevice, pressed: bool) {
    let Some(client) = context.mqtt else {
        return;
    };

    let payload = json!({
        "id": next_message_id(),
        "channel": 0,
        "from": device.node_number,
        "payload": {
            "name": "panic-button",
            "timestamp": Utc::now().timestamp(),
            "time": Utc::now().timestamp(),
            "state": pressed,
            "text": if pressed { "PANIC button pressed" } else { "panic cleared" },
        },
        "sender": device.id(),
        "timestamp": Utc::now().timestamp(),
        "to": -1,
        "type": "detection_sensor",
    });
    publish(client, &topic_for(context, device), payload).await;
}

async fn publish(client: &AsyncClient, topic: &str, payload: Value) {
    if let Err(error) = client
        .publish(topic, QoS::AtLeastOnce, false, payload.to_string())
        .await
    {
        warn!(%error, %topic, "failed to publish MQTT message");
    }
}

fn next_message_id() -> u32 {
    MESSAGE_ID.fetch_add(1, Ordering::Relaxed)
}

fn haversine_m(a: (f64, f64), b: (f64, f64)) -> f64 {
    let earth_radius = 6_371_000.0;
    let lat1 = a.0.to_radians();
    let lat2 = b.0.to_radians();
    let delta_lat = (b.0 - a.0).to_radians();
    let delta_lon = (b.1 - a.1).to_radians();

    let h =
        (delta_lat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (delta_lon / 2.0).sin().powi(2);
    2.0 * earth_radius * h.sqrt().asin()
}

fn bearing(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let lat1 = lat1.to_radians();
    let lat2 = lat2.to_radians();
    let delta_lon = (lon2 - lon1).to_radians();

    let y = delta_lon.sin() * lat2.cos();
    let x = lat1.cos() * lat2.sin() - lat1.sin() * lat2.cos() * delta_lon.cos();
    y.atan2(x).to_degrees().rem_euclid(360.0)
}
