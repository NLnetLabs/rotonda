// Marker trait to have one single `T: impl Outputable` we can work with? And at the same time
// force implementations for all formats?
pub trait Outputable: ToJson + ToCli  { }
impl<T> Outputable for T where T: ToJson + ToCli { }

pub trait ToJson{
    fn to_json(&self, target: impl std::io::Write) ->  Result<(), OutputError>;
}

pub trait ToCli{
    fn to_cli(&self, target: &mut impl std::io::Write) ->  Result<(), OutputError>;
}

pub trait OutputFormat {
    fn write(&mut self, item: impl Outputable) -> Result<(), OutputError>;
}

pub struct JsonFormat<W: std::io::Write>(pub W);

impl<W: std::io::Write> OutputFormat for JsonFormat<W> {
    fn write(&mut self, item: impl Outputable) -> Result<(), OutputError> {
        item.to_json(&mut self.0)
    }
}

pub struct CliFormat<W: std::io::Write>(pub W);

impl<W: std::io::Write> OutputFormat for CliFormat<W> {
    fn write(&mut self, item: impl Outputable) -> Result<(), OutputError> {
        item.to_cli(&mut self.0)
    }
}


pub struct OutputError {
    error_type: OutputErrorType,
}

enum OutputErrorType {
    Other,
}

