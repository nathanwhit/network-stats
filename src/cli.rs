use clap::Parser;

#[derive(Parser)]
#[clap(version = clap::crate_version!(), author = clap::crate_authors!())]
pub struct CliOptions {
    #[clap(short, long, default_value = "8801")]
    pub port: u64,
    #[clap(short, long, default_value = "tcp://0.0.0.0")]
    pub bind_address: String,

    #[clap(short, long, default_value = "")]
    pub seeds: Vec<String>,
}

impl CliOptions {
    pub fn bind_address(&self) -> String {
        format!("{}:{}", self.bind_address, self.port)
    }
}
