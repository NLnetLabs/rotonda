use std::{
    convert::TryInto,
    ffi::OsStr,
    fs::File,
    io::{Read, Write},
    net::TcpStream,
    thread::sleep,
    time::{Duration, Instant},
};

use routecore::bmp::message::Message as BmpMsg;

fn main() {
    const USIZE_BYTES: usize = (usize::BITS as usize) >> 3;

    let mut inputs = Vec::new();
    for entry in std::fs::read_dir("bmp-dump").unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.extension() == Some(OsStr::new("bin")) {
            inputs.push(path);
        }
    }

    let mut join_handles = Vec::new();

    for path in inputs {
        let handle = std::thread::spawn(move || {
            let mut file = File::open(path).unwrap();
            let file_len = file.metadata().unwrap().len() as usize;
            let mut ts_bytes: [u8; 16] = [0; 16];
            let mut num_bmp_bytes: [u8; USIZE_BYTES] = [0; USIZE_BYTES];
            let mut bmp_bytes = Vec::with_capacity(64 * 1024);
            let mut last_ts = None;
            let mut last_push = Instant::now();

            let mut stream = TcpStream::connect("127.0.0.1:11019").unwrap();
            let mut bytes_left = file_len;

            while bytes_left > 0 {
                // Each line has the form <timestamp:u128><num bytes:usize><bmp bytes>
                file.read_exact(&mut ts_bytes).unwrap();
                bytes_left -= ts_bytes.len();
                let ts = u128::from_be_bytes(ts_bytes);

                file.read_exact(&mut num_bmp_bytes).unwrap();
                bytes_left -= num_bmp_bytes.len();
                let num_bytes_to_read = usize::from_be_bytes(num_bmp_bytes);
                bmp_bytes.resize(num_bytes_to_read, 0);

                file.read_exact(&mut bmp_bytes).unwrap();
                bytes_left -= bmp_bytes.len();
                let bmp_msg =
                    BmpMsg::from_octets(bmp_bytes.as_slice()).unwrap();

                // Did the original stream contain the message at this
                // point in time or do we need to wait a bit so as to
                // more closely emulate the original rate at which the
                // data was seen?
                if let Some(last_ts) = last_ts {
                    let millis_between_messages = ts - last_ts;
                    let millis_since_last_push =
                        last_push.elapsed().as_millis();
                    if millis_since_last_push < millis_between_messages {
                        let _millis_to_sleep =
                            millis_between_messages - millis_since_last_push;
                        sleep(Duration::from_millis(
                            _millis_to_sleep.try_into().unwrap(),
                        ));
                    }
                }

                stream.write_all(bmp_msg.as_ref()).unwrap();

                last_ts = Some(ts);
                last_push = Instant::now();
            }
        });

        join_handles.push(handle);
    }

    for handle in join_handles {
        handle.join().unwrap();
    }
}
