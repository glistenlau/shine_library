use std::{fs, path::PathBuf, str::FromStr};

use chrono;

fn get_path(log_path: &str) -> PathBuf {
    let mut path = PathBuf::from_str(log_path).unwrap();
    path.push("_log");
    path
}

pub fn setup_logger(log_path: &str) -> Result<(), fern::InitError> {
    let path = get_path(log_path);
    fs::create_dir_all(&path)?;
    fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "{}[{:?}][{}][{}] {}",
                chrono::Local::now().format("[%Y-%m-%d][%H:%M:%S]"),
                std::thread::current().id(),
                record.target(),
                record.level(),
                message
            ))
        })
        .level(log::LevelFilter::Debug)
        // .chain(std::io::stdout())
        .chain(fern::log_file(path)?)
        .apply()?;
    Ok(())
}
