mod fields;
mod sift;
mod state;

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rumqttc::tokio_rustls::rustls::{
    self, DigitallySignedStruct, SignatureScheme,
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    pki_types::{CertificateDer, ServerName, UnixTime},
};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS, TlsConfiguration, Transport};
use serde_json::Value;

#[derive(Debug)]
struct AcceptAnyCert;

impl ServerCertVerifier for AcceptAnyCert {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::ED25519,
        ]
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    let host = std::env::var("PRINTER_HOST")?;
    let serial = std::env::var("PRINTER_SERIAL")?;
    let access_code = std::env::var("PRINTER_ACCESS_CODE")?;
    let sift_uri =
        std::env::var("SIFT_EDGE_URI").unwrap_or_else(|_| "grpc://localhost:6666".to_string());
    let sift_asset =
        std::env::var("SIFT_EDGE_ASSET").unwrap_or_else(|_| "bambu_printer".to_string());

    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let tls = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(AcceptAnyCert))
        .with_no_client_auth();

    let mut opts = MqttOptions::new("bambu-stream", host, 8883);
    opts.set_credentials("bblp", &access_code);
    opts.set_keep_alive(Duration::from_secs(30));
    opts.set_transport(Transport::tls_with_config(TlsConfiguration::Rustls(
        Arc::new(tls),
    )));

    let (client, mut eventloop) = AsyncClient::new(opts, 32);

    let report_topic = format!("device/{serial}/report");
    let request_topic = format!("device/{serial}/request");

    client.subscribe(&report_topic, QoS::AtMostOnce).await?;
    client
        .publish(
            &request_topic,
            QoS::AtMostOnce,
            false,
            r#"{"pushing":{"sequence_id":"0","command":"pushall"}}"#,
        )
        .await?;

    let schema = fields::schema();
    let mut writer = sift::SiftEdgeWriter::connect(&sift_uri, &sift_asset, &schema).await?;
    println!("connected to sift edge at {sift_uri} (asset: {sift_asset})");

    println!("subscribed to {report_topic}");

    let mut snapshot = Value::Object(Default::default());

    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Packet::Publish(p))) => {
                match serde_json::from_slice::<Value>(&p.payload) {
                    Ok(delta) => {
                        state::deep_merge(&mut snapshot, delta);

                        let print_state = snapshot.get("print").unwrap_or(&Value::Null);
                        let (ts_ms, ts_ns) = now_ms_ns();
                        let row = fields::PrinterRow::extract(print_state, ts_ms, ts_ns);
                        println!("{row:?}");
                        let batch = fields::to_batch(schema.clone(), &[row])?;
                        if let Err(e) = writer.push(&batch).await {
                            eprintln!("sift edge push error: {e}");
                        }
                    }
                    Err(e) => eprintln!("parse error: {e}"),
                }
            }
            Ok(_) => {}
            Err(e) => {
                eprintln!("eventloop error: {e:?}");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
}

fn now_ms_ns() -> (i64, i32) {
    let total_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let ms = (total_ns / 1_000_000) as i64;
    let ns = (total_ns % 1_000_000) as i32;
    (ms, ns)
}
