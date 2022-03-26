use std::{fs, path::PathBuf, str::FromStr};

use chrono;

fn get_path(log_path: &str) -> PathBuf {
    let mut path = PathBuf::from_str(log_path).unwrap();
    path.push("_log");
    path
}

pub fn setup_logger(log_path: &str) -> Result<(), fern::InitError> {
    let path = get_path(log_path);
    fs::create_dir_all(&log_path)?;
    let mut dispath = fern::Dispatch::new()
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
        .chain(fern::log_file(path)?);

    if cfg!(debug_assertions) {
        dispath = dispath
            .chain(std::io::stdout())
            .level(log::LevelFilter::Debug);
    } else {
        dispath = dispath.level(log::LevelFilter::Info);
    }

    dispath.apply()?;
    Ok(())
}
