use clap::Parser;

#[derive(Parser)]
#[clap(version = clap::crate_version!(), author = clap::crate_authors!())]
pub struct CliOptions {
    #[clap(short, long, default_value = "8801")]
    port: u64,

    #[clap(short, long, default_value = "tcp://0.0.0.0")]
    bind_address: String,

    #[clap(short, long, default_value = "tcp://127.0.0.1")]
    endpoint: String,

    #[clap(short, long, default_value = "")]
    pub seeds: Vec<String>,

    #[clap(long)]
    pub dot: Option<String>,

    #[clap(long)]
    pub graphml: Option<String>,
}

impl CliOptions {
    pub fn bind_address(&self) -> String {
        format!("{}:{}", self.bind_address, self.port)
    }
    pub fn endpoint(&self) -> String {
        format!("{}:{}", self.endpoint, self.port)
    }
}
