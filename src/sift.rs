use anyhow::{Context, Result, bail};
use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use arrow_flight::flight_descriptor::DescriptorType;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{FlightData, FlightDescriptor, encode::FlightDataEncoderBuilder, error::FlightError};
use futures::StreamExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;

pub struct SiftEdgeWriter {
    batch_tx: mpsc::Sender<Result<RecordBatch, FlightError>>,
}

impl SiftEdgeWriter {
    pub async fn connect(uri: &str, asset: &str, _schema: &Schema) -> Result<Self> {
        let endpoint = normalize_uri(uri)?;
        let channel = Channel::from_shared(endpoint.clone())
            .with_context(|| format!("invalid sift edge uri: {endpoint}"))?
            .connect()
            .await
            .with_context(|| format!("failed to connect to sift edge at {endpoint}"))?;
        let mut client = FlightServiceClient::new(channel);

        let descriptor = FlightDescriptor {
            r#type: DescriptorType::Path.into(),
            path: vec![asset.to_string()],
            ..Default::default()
        };

        let (batch_tx, batch_rx) = mpsc::channel::<Result<RecordBatch, FlightError>>(8);
        let (flight_tx, flight_rx) = mpsc::channel::<FlightData>(64);

        let encoded_stream = FlightDataEncoderBuilder::new()
            .with_flight_descriptor(Some(descriptor))
            .build(ReceiverStream::new(batch_rx));

        tokio::spawn(async move {
            tokio::pin!(encoded_stream);
            while let Some(result) = encoded_stream.next().await {
                match result {
                    Ok(data) => {
                        if flight_tx.send(data).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        eprintln!("sift edge encoding error: {e}");
                        break;
                    }
                }
            }
        });

        tokio::spawn(async move {
            match client.do_put(ReceiverStream::new(flight_rx)).await {
                Ok(response) => {
                    let mut response = response.into_inner();
                    while let Some(msg) = response.next().await {
                        if let Err(e) = msg {
                            eprintln!("sift edge response error: {e}");
                        }
                    }
                }
                Err(e) => eprintln!("sift edge do_put failed: {e}"),
            }
        });

        Ok(Self { batch_tx })
    }

    pub async fn push(&mut self, batch: &RecordBatch) -> Result<()> {
        self.batch_tx
            .send(Ok(batch.clone()))
            .await
            .map_err(|_| anyhow::anyhow!("sift edge channel closed"))?;
        Ok(())
    }
}

fn normalize_uri(uri: &str) -> Result<String> {
    if let Some(rest) = uri.strip_prefix("grpc://") {
        Ok(format!("http://{rest}"))
    } else if let Some(rest) = uri.strip_prefix("grpc+tls://") {
        Ok(format!("https://{rest}"))
    } else if uri.starts_with("http://") || uri.starts_with("https://") {
        Ok(uri.to_string())
    } else {
        bail!(
            "unsupported sift edge uri scheme (use grpc://, grpc+tls://, http://, or https://): {uri}"
        )
    }
}
