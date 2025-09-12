pub mod cur_columns;

use anyhow::Result;
use arrow_array::{Float64Array, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;

pub fn create_batches() -> Result<(Arc<Schema>, Vec<RecordBatch>)> {
    // 1. Define the schema for our data.
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Float64, false),
        Field::new("description", DataType::Utf8, false),
    ]));

    // 2. Create the first batch of data.
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Float64Array::from(vec![10.1, 20.2, 30.3])),
            Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
        ],
    )?;

    // 3. Create the second batch of data.
    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![4, 5])),
            Arc::new(Float64Array::from(vec![40.4, 50.5])),
            Arc::new(StringArray::from(vec!["qux", "quux"])),
        ],
    )?;

    Ok((schema, vec![batch1, batch2]))
}
