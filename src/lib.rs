use std::io;
use std::sync::Arc;

use io::BufWriter;
use io::Write;

use futures::StreamExt;
use futures::TryStreamExt;

use arrow::datatypes::Schema;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

use datafusion::dataframe::DataFrame;
use datafusion::execution::SendableRecordBatchStream;

pub struct IpcStreamWriter<W: Write>(pub StreamWriter<BufWriter<W>>);

impl<W> IpcStreamWriter<W>
where
    W: Write,
{
    pub async fn write_batch_stream(
        self,
        strm: SendableRecordBatchStream,
    ) -> Result<Self, io::Error> {
        let mut me: Self = strm
            .map(|r| r.map_err(io::Error::other))
            .try_fold(self, |mut state, next| async move {
                let rbat: &RecordBatch = &next;
                state.0.write(rbat).map_err(io::Error::other)?;
                Ok(state)
            })
            .await?;
        me.0.flush().map_err(io::Error::other)?;
        Ok(me)
    }

    pub async fn df2writer(self, df: DataFrame) -> Result<Self, io::Error> {
        let strm = df.execute_stream().await?;
        self.write_batch_stream(strm).await
    }
}

pub async fn df2stdout(df: DataFrame) -> Result<(), io::Error> {
    let mut o = io::stdout().lock();
    let sch: Arc<Schema> = df.schema().inner().clone();
    let swtr: StreamWriter<_> =
        StreamWriter::try_new_buffered(&mut o, &sch).map_err(io::Error::other)?;
    let mut iwtr: IpcStreamWriter<_> = IpcStreamWriter(swtr).df2writer(df).await?;
    iwtr.0.finish().map_err(io::Error::other)?;
    drop(iwtr);
    o.flush()?;
    Ok(())
}
