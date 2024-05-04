use std::env;
use std::sync::Arc;
use datafusion::dataframe::DataFrameWriteOptions;

use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::error::Result;
use datafusion::prelude::*;
use datafusion_common::{FileType, GetExt};

use object_store::aws::AmazonS3Builder;
use url::Url;

#[tokio::main]
async fn main() -> Result<()> {
    // load environment variables
    dotenv::dotenv().ok();

    // create local execution context
    let ctx = SessionContext::new();

    // define region and repository to which your credentials have GET and PUT access
    let region = "us-east-1";
    let repo = "demo";
    let branch = "main";

    let s3a = AmazonS3Builder::new()
        .with_bucket_name(repo)
        .with_region(region)
        .with_access_key_id(env::var("AWS_ACCESS_KEY_ID").unwrap())
        .with_secret_access_key(env::var("AWS_SECRET_ACCESS_KEY").unwrap())
        .with_endpoint("http://localhost:8000")
        .with_allow_http(true) // enable http endpoints
        .build()?;

    let path = format!("s3a://{repo}");
    let s3a_url = Url::parse(&path).unwrap();
    let arc_s3a = Arc::new(s3a);
    ctx.runtime_env()
        .register_object_store(&s3a_url, arc_s3a.clone());

    let path = format!("s3a://{repo}/{branch}/taxi_data/input/yellow_tripdata_2022-02.parquet");
    let file_format = ParquetFormat::default().with_enable_pruning(true);
    let listing_options = ListingOptions::new(Arc::new(file_format))
        .with_file_extension(FileType::PARQUET.get_ext());
    ctx.register_listing_table("taxi_table", &path, listing_options, None, None)
        .await?;

    let df = ctx.sql("SELECT * from taxi_table").await?;

    // write as parquet to lakefs
    let out_path = format!("s3a://{repo}/{branch}/taxi_data/output/parquet/");
    df.clone().write_parquet(&out_path, DataFrameWriteOptions::new(), None).await?;

    // write as JSON to lakefs
    let json_out = format!("s3a://{repo}/{branch}/taxi_data/output/json");
    df.clone().write_json(&json_out, DataFrameWriteOptions::new(), None).await?;

    // write as csv to lakefs
    let csv_out = format!("s3a://{repo}/{branch}/taxi_data/output/csv");
    df.write_csv(&csv_out, DataFrameWriteOptions::new(), None).await?;

    let file_format = ParquetFormat::default().with_enable_pruning(true);
    let listing_options = ListingOptions::new(Arc::new(file_format))
        .with_file_extension(FileType::PARQUET.get_ext());
    ctx.register_listing_table("taxi_table2", &out_path, listing_options, None, None)
        .await?;

    let df = ctx
        .sql("SELECT * FROM taxi_table2")
        .await?;

    df.show_limit(20).await?;
    Ok(())
}
