use std::{fs, io::Write, process::{Child, Command, Stdio}, thread, time::Duration};

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
    String::from_utf8_lossy(&output.stdout).into()
}

fn jq_post_processing(jq_cmd: impl AsRef<str>, input: String) -> String {

    let mut cmd = Command::new("jq")
        .arg(jq_cmd.as_ref())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("Failed to spawn jq");

    let mut stdin = cmd.stdin.take().expect("Failed to open stdin");
    std::thread::spawn(move || {
        stdin.write_all(input.as_bytes()).expect("Failed to write to stdin");
    });

    let output = cmd.wait_with_output().expect("Failed to read stdout");
    String::from_utf8_lossy(&output.stdout).to_string()
}

fn socat(input: impl AsRef<str>) {
    Command::new("socat")
        .arg("TCP:localhost:11019")
        .arg(format!("FILE:{}", input.as_ref()))
        .status().unwrap();
}

fn socat_ignore_eof(input: impl AsRef<str>) -> Child {
    let mut process = Command::new("socat")
        .arg("TCP:localhost:11019")
        .arg(format!("FILE:{},ignoreeof", input.as_ref()))
        .spawn().unwrap();
    process
}


//#[test]
//fn http_api_responses() {
//    glob!("http_api_responses/**/*.get", |url| {
//        let base_dir = url.parent().unwrap();
//        let rotonda_conf = base_dir.join("rotonda.conf");
//        dbg!(&url);
//        dbg!(&base_dir);
//        dbg!(&rotonda_conf);
//        let mut cmd = Command::cargo_bin("rotonda").unwrap();
//        dbg!(&cmd);
//        let mut rotonda_process = cmd
//            .args(["--config", rotonda_conf.to_string_lossy().as_ref()])
//            .spawn()
//            .unwrap()
//            ;
//        dbg!(&rotonda_process);
//        wait(3);
//
//        let mut socat_processes = vec![];
//        for raw_bmp in glob::glob(format!("{}/*.bmp", base_dir.to_string_lossy()).as_ref())
//            .unwrap().filter_map(Result::ok)
//        {
//            dbg!(&raw_bmp);
//            let handle = socat_ignore_eof(raw_bmp.to_string_lossy().as_ref());
//            socat_processes.push(handle);
//        }
//        if !socat_processes.is_empty() {
//            wait(5);
//            eprintln!("post waiting on socat_processes");
//
//        }
//
//        assert_snapshot!(curl_get(fs::read_to_string(url).unwrap()));
//
//        wait(5);
//        eprintln!("waited 5 secs, killing rotonda");
//        let _ = rotonda_process.kill();
//        let _ = rotonda_process.wait();
//
//    });
//}

#[test]
fn http_api_responses_multiple_gets() {
    for entry in glob::glob("tests/http_api_responses/*").unwrap() {
        let base_dir = match entry {
            Ok(path) => {
                if path.is_dir() {
                    let mut components = path.components();
                    components.next(); // chop off "tests/"
                    components.next(); // chop off "http_api_responses/"
                    if let Some(comp) = components.next() {
                        match comp {
                            std::path::Component::Normal(os_str) => {
                                eprintln!("looking at {:?}", &os_str);
                                if os_str.to_string_lossy().starts_with("IGNORE_") {
                                    eprintln!("ignoring {:?}", os_str);
                                    continue
                                }
                            }
                            _ => panic!()
                        }
                    } else {
                        panic!()
                    }
                    path
                } else {
                    continue
                }
            }
            Err(_) => panic!(),
        };
        // do one-time setup for case_dir
        eprintln!("performing setup for {}", base_dir.to_string_lossy());

        //let base_dir = base_dir.clone(); //url.parent().unwrap();
        let rotonda_conf = base_dir.join("rotonda.conf");

        eprintln!("base_dir {:?}", &base_dir);
        eprintln!("rotonda_conf: {:?}", &rotonda_conf);

        let mut cmd = Command::cargo_bin("rotonda").unwrap();
        //dbg!(&cmd);

        let mut rotonda_process = cmd
            .args(["--config", rotonda_conf.to_string_lossy().as_ref()])
            .spawn()
            .unwrap()
            ;
        //dbg!(&rotonda_process);

        wait(3);

        let mut socat_processes = vec![];
        for raw_bmp in glob::glob(format!("{}/*.bmp", base_dir.to_string_lossy()).as_ref())
            .unwrap().filter_map(Result::ok)
            {
                //dbg!(&raw_bmp);
                let handle = socat_ignore_eof(raw_bmp.to_string_lossy().as_ref());
                socat_processes.push(handle);
            }
        if !socat_processes.is_empty() {
            wait(5);
            eprintln!("post waiting on socat_processes");

        }

        let case_dir = {
            let mut components = base_dir.components();
            components.next(); // chop off tests/
            components.as_path().to_owned()
        };
        // now do all the HTTP GETs

        // insta::glob! works from tests/ as its basedir

        glob!(&format!("{}/*.get", case_dir.to_string_lossy()), |url| {
            let all = fs::read_to_string(url).unwrap();
            let mut lines = all.lines();
            let url = lines.next().unwrap();
            let mut raw_output = curl_get(url);
            if let Some(jq_cmd) = lines.next() {
                raw_output = jq_post_processing(jq_cmd, raw_output);
            }
            assert_snapshot!(raw_output);

        });
        wait(5);
        eprintln!("waited 5 secs, killing rotonda");
        let _ = rotonda_process.kill();
        let _ = rotonda_process.wait();

    }
}
