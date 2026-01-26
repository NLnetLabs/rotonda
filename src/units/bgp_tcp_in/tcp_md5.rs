use std::io;
use std::net::IpAddr;

use tokio::net::{TcpListener, TcpStream};

#[cfg(target_os = "linux")]
use std::os::unix::io::AsRawFd;

use crate::common::net::StandardTcpListener;
use super::ListenerMd5Config;

#[cfg(target_os = "linux")]
const TCP_MD5SIG_MAXKEYLEN: usize = 80;

#[cfg(target_os = "linux")]
const TCP_MD5SIG_FLAG_PREFIX: u8 = 0x1;

#[cfg(target_os = "linux")]
#[repr(C)]
struct TcpMd5Sig {
    tcpm_addr: libc::sockaddr_storage,
    tcpm_flags: u8,
    tcpm_prefixlen: u8,
    tcpm_keylen: u16,
    tcpm_ifindex: i32,
    tcpm_key: [u8; TCP_MD5SIG_MAXKEYLEN],
}

#[cfg(target_os = "linux")]
fn sockaddr_storage_from_ip(addr: IpAddr) -> libc::sockaddr_storage {
    let mut storage: libc::sockaddr_storage =
        unsafe { std::mem::zeroed() };

    match addr {
        IpAddr::V4(ip) => {
            let v4 = libc::sockaddr_in {
                sin_family: libc::AF_INET as libc::sa_family_t,
                sin_port: 0,
                sin_addr: libc::in_addr {
                    s_addr: u32::from(ip).to_be(),
                },
                sin_zero: [0; 8],
            };
            unsafe {
                std::ptr::copy_nonoverlapping(
                    &v4 as *const _ as *const u8,
                    &mut storage as *mut _ as *mut u8,
                    std::mem::size_of::<libc::sockaddr_in>(),
                );
            }
        }
        IpAddr::V6(ip) => {
            let v6 = libc::sockaddr_in6 {
                sin6_family: libc::AF_INET6 as libc::sa_family_t,
                sin6_port: 0,
                sin6_flowinfo: 0,
                sin6_addr: libc::in6_addr { s6_addr: ip.octets() },
                sin6_scope_id: 0,
            };
            unsafe {
                std::ptr::copy_nonoverlapping(
                    &v6 as *const _ as *const u8,
                    &mut storage as *mut _ as *mut u8,
                    std::mem::size_of::<libc::sockaddr_in6>(),
                );
            }
        }
    }

    storage
}

#[cfg(target_os = "linux")]
fn configure_md5_fd(
    fd: libc::c_int,
    addr: IpAddr,
    prefix_len: Option<u8>,
    key: &[u8],
) -> io::Result<()> {
    let family = listener_family(fd)?;
    match (family, addr) {
        (val, IpAddr::V6(_))
            if val == libc::AF_INET as libc::sa_family_t =>
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "TCP listener is IPv4, cannot set TCP MD5 for IPv6 peer",
            ));
        }
        (val, IpAddr::V4(_))
            if val == libc::AF_INET6 as libc::sa_family_t =>
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "TCP listener is IPv6, cannot set TCP MD5 for IPv4 peer",
            ));
        }
        _ => {}
    }

    // RFC 2385 p3: "The MD5 digest is always 16 bytes in length, and the
    // option would appear in every segment of a connection."
    if key.len() > TCP_MD5SIG_MAXKEYLEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "TCP MD5 key length exceeds 80 bytes",
        ));
    }

    let mut md5sig = TcpMd5Sig {
        tcpm_addr: sockaddr_storage_from_ip(addr),
        tcpm_flags: 0,
        tcpm_prefixlen: 0,
        tcpm_keylen: key.len() as u16,
        tcpm_ifindex: 0,
        tcpm_key: [0u8; TCP_MD5SIG_MAXKEYLEN],
    };

    if let Some(prefix_len) = prefix_len {
        md5sig.tcpm_flags = TCP_MD5SIG_FLAG_PREFIX;
        md5sig.tcpm_prefixlen = prefix_len;
    }

    md5sig.tcpm_key[..key.len()].copy_from_slice(key);

    let ret = unsafe {
        libc::setsockopt(
            fd,
            libc::IPPROTO_TCP,
            libc::TCP_MD5SIG,
            &md5sig as *const _ as *const libc::c_void,
            std::mem::size_of::<TcpMd5Sig>() as libc::socklen_t,
        )
    };

    if ret == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(target_os = "linux")]
fn listener_family(fd: libc::c_int) -> io::Result<libc::sa_family_t> {
    let mut storage: libc::sockaddr_storage =
        unsafe { std::mem::zeroed() };
    let mut len = std::mem::size_of::<libc::sockaddr_storage>()
        as libc::socklen_t;
    let ret = unsafe {
        libc::getsockname(
            fd,
            &mut storage as *mut _ as *mut libc::sockaddr,
            &mut len,
        )
    };
    if ret == 0 {
        Ok(storage.ss_family as libc::sa_family_t)
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(target_os = "linux")]
pub fn configure_tcp_md5(
    stream: &TcpStream,
    addr: IpAddr,
    key: &[u8],
) -> io::Result<()> {
    configure_md5_fd(stream.as_raw_fd(), addr, None, key)
}

#[cfg(target_os = "linux")]
pub fn configure_tcp_md5_listener(
    listener: &TcpListener,
    addr: IpAddr,
    prefix_len: Option<u8>,
    key: &[u8],
) -> io::Result<()> {
    // RFC 2385 p2: "there is no negotiation for the use of this option in a
    // connection... [it is] purely a matter of site policy."
    configure_md5_fd(listener.as_raw_fd(), addr, prefix_len, key)
}

#[cfg(not(target_os = "linux"))]
pub fn configure_tcp_md5(
    _stream: &TcpStream,
    _addr: IpAddr,
    _key: &[u8],
) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "TCP MD5 is only supported on Linux",
    ))
}

#[cfg(not(target_os = "linux"))]
pub fn configure_tcp_md5_listener(
    _listener: &TcpListener,
    _addr: IpAddr,
    _prefix_len: Option<u8>,
    _key: &[u8],
) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "TCP MD5 is only supported on Linux",
    ))
}

impl ListenerMd5Config for StandardTcpListener {
    fn configure_md5(
        &self,
        addr: IpAddr,
        prefix_len: Option<u8>,
        key: &[u8],
    ) -> io::Result<()> {
        configure_tcp_md5_listener(&self.0, addr, prefix_len, key)
    }
}
