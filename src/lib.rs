pub use clap;

use clap::{Parser, Subcommand};
use std::{
    path::PathBuf,
};

pub mod topology;



#[derive(Parser)]
#[command(author, version, about)]
struct App<T>
where T: Subcommand
{
    #[command(subcommand)]
    command: Commands<T>,
}

#[derive(Subcommand)]
enum Commands<T>
where T: Subcommand
{
    //Supertop {},
    Topograf(TopoConf),

    #[command(flatten)]
    AppSubCommands(T),
}

#[derive(Debug,Parser)]
struct TopoConf {
    #[arg(long)]
    host: String,
    #[arg(short,long,value_name="TMP_DIR")]
    tmp: PathBuf,
}


pub fn run<T>() -> T
where T: Subcommand
{
    let app = App::parse();
    match app.command {
        Commands::Topograf(conf) => {
            panic!("EXEC: topograf {:?}",conf);
        },
        Commands::AppSubCommands(t) => t,
    }
}

