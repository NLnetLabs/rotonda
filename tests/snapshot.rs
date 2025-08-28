use std::{fs, process::{Child, Command}, thread, time::Duration};

use assert_cmd::prelude::*;
use insta::{assert_debug_snapshot, assert_snapshot, glob};


fn wait(seconds: u64) {
    thread::sleep(Duration::from_secs(seconds))
}

fn curl_get(url: impl AsRef<str>) -> String {
    let url = format!("http://localhost:8081{}", url.as_ref().trim());
    dbg!(&url);
    let output = Command::new("curl")
        .arg(url)
        .output().unwrap();
    dbg!(&output);
    String::from_utf8_lossy(&output.stdout).into()
}

fn socat(input: impl AsRef<str>) {
    Command::new("socat")
        .arg("TCP:localhost:11019")
        .arg(format!("FILE:{}", input.as_ref()))
        //.spawn().unwrap();
        .status().unwrap();
}

fn socat_ignore_eof(input: impl AsRef<str>) -> Child {
    let mut process = Command::new("socat")
        .arg("TCP:localhost:11019")
        .arg(format!("FILE:{},ignoreeof", input.as_ref()))
        .spawn().unwrap();
    process
}


#[test]
fn http_api_responses() {
    glob!("http_api_responses/**/*.get", |url| {
        let base_dir = url.parent().unwrap();
        let rotonda_conf = base_dir.join("rotonda.conf");
        dbg!(&url);
        dbg!(&base_dir);
        dbg!(&rotonda_conf);
        let mut cmd = Command::cargo_bin("rotonda").unwrap();
        dbg!(&cmd);
        let mut rotonda_process = cmd
            .args(["--config", rotonda_conf.to_string_lossy().as_ref()])
            .spawn()
            .unwrap()
            ;
        dbg!(&rotonda_process);
        wait(3);

        let mut socat_processes = vec![];
        for raw_bmp in glob::glob(format!("{}/*.bmp", base_dir.to_string_lossy()).as_ref())
            .unwrap().filter_map(Result::ok)
        {
            dbg!(&raw_bmp);
            let handle = socat_ignore_eof(raw_bmp.to_string_lossy().as_ref());
            socat_processes.push(handle);
        }
        if !socat_processes.is_empty() {
            wait(5);
            eprintln!("post waiting on socat_processes");

        }

        eprintln!("pre curl_get");
        assert_snapshot!(curl_get(fs::read_to_string(url).unwrap()));
        eprintln!("post curl_get");

        wait(5);
        eprintln!("waited 5 secs, killing rotonda");
        let _ = rotonda_process.kill();
        let _ = rotonda_process.wait();

    });
}
