# bambu-stream

MQTT client for Bambu Lab printers, in Rust. Connects over LAN, merges incremental state deltas into a full snapshot, and streams telemetry to [Sift Edge](https://siftstack.com) via Arrow Flight.

## Setup

Requires Rust 1.85+ and a Bambu printer in LAN mode.

Create a `.env` file with:

- `PRINTER_HOST` — printer's IP (Settings → WLAN)
- `PRINTER_ACCESS_CODE` — Settings → WLAN → Access Code
- `PRINTER_SERIAL` — Settings → Device → SN
- `SIFT_EDGE_URI` — optional, defaults to `grpc://localhost:6666`
- `SIFT_EDGE_ASSET` — optional, defaults to `bambu_printer`

## Run

```
cargo run --release
```

The client connects to the printer's MQTT broker, sends `pushall` to prime full state, and forwards hydrated rows to Sift Edge as they arrive (~1 Hz idle, faster during prints).

## Layout

- `src/main.rs` — wires MQTT, state, and Sift writer together
- `src/state.rs` — recursive deep-merge of MQTT deltas into a full snapshot
- `src/fields.rs` — schema and extraction of telemetry columns
- `src/sift.rs` — Arrow Flight `do_put` client
