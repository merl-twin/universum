#[derive(Debug,clap::Parser)]
struct Args1 {
    #[arg(long)]
    host: String,
}

#[derive(Debug,clap::Parser)]
struct Args2 {
    #[arg(short,long,value_name="TMP_DIR")]
    tmp: String,
}

#[derive(clap::Subcommand)]
enum Commands {
    Cmd1(Args1),
    Cmd2(Args2),
    Cmd3,
}


/*fn main_result(uni: &mut Universum<Commands>) -> Result<(),String> {
    println!("Run: {:?}",uni.command());


    Ok(())
}*/



fn main() -> Result<(),String> {
    match  universum::run() {
        Commands::Cmd1(a1) => println!("Cmd1: {:?}",a1),
        Commands::Cmd2(a2) => println!("Cmd2: {:?}",a2),
        Commands::Cmd3 => println!("Cmd3"),
    }
    Ok(())
}

