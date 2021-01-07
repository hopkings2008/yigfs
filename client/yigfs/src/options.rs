use clap::{App, Arg};

#[derive(Debug, Default)]
pub struct Options{
    // region
    pub region: String,
    // bucket name
    pub bucket: String,
    // access key
    pub ak: String,
    // secret key
    pub sk: String,
    // mount point path
    pub mnt: String,
}

pub fn parse() -> Options {
    let matches = App::new("yigfs")
    .arg(Arg::with_name("mnt")
        .help("mount point path")
        .short("m")
        .long("mnt")
        .required(true)
        .takes_value(true))
    .arg(Arg::with_name("region")
        .help("region")
        .short("r")
        .long("region")
        .required(true)
        .takes_value(true))
    .arg(Arg::with_name("bucket")
        .help("bucket name")
        .short("b")
        .long("bucket")
        .required(true)
        .takes_value(true))
    .arg(Arg::with_name("ak")
        .help("access key")
        .short("a")
        .long("ak")
        .required(true)
        .takes_value(true))
    .arg(Arg::with_name("sk")
        .help("secret key")
        .short("s")
        .long("sk")
        .required(true)
        .takes_value(true)).get_matches();

    let mut opts: Options = Default::default();
    if let Some(value) = matches.value_of("region"){
        opts.region = String::from(value);
    }
    if let Some(value) = matches.value_of("bucket"){
        opts.bucket = String::from(value);
    }
    if let Some(value) = matches.value_of("ak") {
        opts.ak = String::from(value);
    }
    if let Some(value) = matches.value_of("sk") {
        opts.sk = String::from(value);
    }
    if let Some(value) = matches.value_of("mnt"){
        opts.mnt = String::from(value);
    }
    opts
}