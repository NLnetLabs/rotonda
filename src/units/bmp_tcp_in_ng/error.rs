use std::{borrow::Cow, fmt};

#[derive(Debug)]
pub struct BmpNgError {
    msg: Cow<'static, str>
}

impl BmpNgError {
    pub fn new(msg: Cow<'static, str>) -> Self {
        Self { msg }
    }
}

impl fmt::Display for BmpNgError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BmpNgError: {}", self.msg)
    }
}

impl std::error::Error for BmpNgError { }


impl<T> From<T> for BmpNgError
where T: Into<Cow<'static, str>>
{

    fn from(value: T) -> Self {
        Self::new(value.into())
    }
}
