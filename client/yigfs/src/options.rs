use clap::{App, Arg};

#[derive(Debug, Default)]
pub struct Options{
    // region
    pub config_file_path: String,
}

pub fn parse() -> Options {
    let matches = App::new("yigfs")
    .arg(Arg::with_name("config")
        .help("config file path")
        .short("c")
        .long("config")
        .required(true)
        .takes_value(true))
    .get_matches();

    let mut opts: Options = Default::default();
    if let Some(value) = matches.value_of("config"){
        opts.config_file_path = String::from(value);
    }
    
    opts
}