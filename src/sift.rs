use anyhow::{Context, Result, bail};
use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use arrow::ipc::writer::{
    CompressionContext, DictionaryTracker, IpcDataGenerator, IpcWriteOptions,
};
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{FlightData, FlightDescriptor, SchemaAsIpc};
use futures::StreamExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;

pub struct SiftEdgeWriter {
    tx: mpsc::Sender<FlightData>,
    options: IpcWriteOptions,
    dictionary_tracker: DictionaryTracker,
    data_gen: IpcDataGenerator,
    compression: CompressionContext,
}

impl SiftEdgeWriter {
    pub async fn connect(uri: &str, asset: &str, schema: &Schema) -> Result<Self> {
        let endpoint = normalize_uri(uri)?;
        let channel = Channel::from_shared(endpoint.clone())
            .with_context(|| format!("invalid sift edge uri: {endpoint}"))?
            .connect()
            .await
            .with_context(|| format!("failed to connect to sift edge at {endpoint}"))?;
        let mut client = FlightServiceClient::new(channel);

        let (tx, rx) = mpsc::channel::<FlightData>(64);
        let stream = ReceiverStream::new(rx);

        let options = IpcWriteOptions::default();

        let mut schema_data: FlightData = SchemaAsIpc::new(schema, &options).into();
        schema_data.flight_descriptor = Some(FlightDescriptor::new_path(vec![asset.to_string()]));
        tx.send(schema_data)
            .await
            .map_err(|_| anyhow::anyhow!("schema send failed"))?;

        tokio::spawn(async move {
            match client.do_put(stream).await {
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

        Ok(Self {
            tx,
            options,
            dictionary_tracker: DictionaryTracker::new(false),
            data_gen: IpcDataGenerator::default(),
            compression: CompressionContext::default(),
        })
    }

    pub async fn push(&mut self, batch: &RecordBatch) -> Result<()> {
        let (encoded_dicts, encoded_batch) = self.data_gen.encode(
            batch,
            &mut self.dictionary_tracker,
            &self.options,
            &mut self.compression,
        )?;

        for d in encoded_dicts {
            self.tx
                .send(d.into())
                .await
                .map_err(|_| anyhow::anyhow!("sift edge channel closed"))?;
        }
        self.tx
            .send(encoded_batch.into())
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
