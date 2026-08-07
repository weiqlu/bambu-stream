# Inspecting Bambu MQTT payloads

How to read the printer's raw MQTT feed with the mosquitto clients.

## Connection

| Setting | Value |
|---|---|
| Host | `$PRINTER_HOST` |
| Port | `8883` (TLS) |
| Username | `bblp` |
| Password | `$PRINTER_ACCESS_CODE` |
| Report topic | `device/$PRINTER_SERIAL/report` |
| Request topic | `device/$PRINTER_SERIAL/request` |
| QoS | 0 |


## 1. Get the printer's certificate

The printer's TLS certificate is self-signed. `src/main.rs` handles this by disabling
verification outright with a custom rustls verifier (`AcceptAnyCert`).

mosquitto has no equivalent switch. `--insecure` only relaxes the *hostname* check — TLS still
needs a CA file via `--cafile` or `--capath`. So fetch the printer's own certificate and use it
as its own trust anchor:

```bash
openssl s_client -connect "$PRINTER_HOST:8883" -showcerts </dev/null 2>/dev/null \
  | awk '/BEGIN CERTIFICATE/,/END CERTIFICATE/' > printer.crt
```

Check what you got:

```bash
openssl x509 -in printer.crt -noout -subject -issuer -dates
```

`--insecure` is still required alongside `--cafile`, because the certificate's subject won't
match the printer's IP address.

## 2. Subscribe

```bash
mosquitto_sub -h "$PRINTER_HOST" -p 8883 \
  --cafile printer.crt --insecure \
  -u bblp -P "$PRINTER_ACCESS_CODE" \
  -i bambu-inspect-sub \
  -t "device/$PRINTER_SERIAL/report" -q 0 \
  --pretty
```

## 3. Prime a full state dump

Reports are **sparse deltas**, not snapshots. A single message typically carries a handful of
keys.

The one message that contains everything is the response to `pushall`. Send it from a second
shell while the subscriber is running:

```bash
mosquitto_pub -h "$PRINTER_HOST" -p 8883 \
  --cafile printer.crt --insecure \
  -u bblp -P "$PRINTER_ACCESS_CODE" \
  -i bambu-inspect-pub \
  -t "device/$PRINTER_SERIAL/request" \
  -m '{"pushing":{"sequence_id":"0","command":"pushall"}}'
```

If nothing comes back, try the longer form — some firmware wants the extra fields, or a
nonzero and increasing `sequence_id`:

```json
{"pushing":{"sequence_id":"1","command":"pushall","version":1,"push_target":1}}
```
