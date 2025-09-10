//! Typed output buffers for several representations (JSON, CLI, HTML)

pub trait GenOutput<T> {
    fn write(&self, target: &mut T) -> Result<(), OutputError>;
}

pub struct Json<W: std::io::Write>(pub W);
pub struct Cli<W: std::io::Write>(pub W);
// For Html, GenOutput is implemented on the specific Html structs (deriving RsHtml) directly.


/// Generate a GenOutput<Json<_>> using serde_json.
///
/// Usage:
///     
///     // Will serialize SomeType
///     genoutput_json!(SomeType)
///
///     // Will serialize SomeWrapperType.0
///     genoutput_json!(SomeWrapperType, 0)
#[macro_export]
macro_rules! genoutput_json {
    ($type:ty $(, $val:tt )? ) => {
        impl<W: std::io::Write> GenOutput<Json<W>> for $type {
            fn write(&self, target: &mut Json<W>) -> Result<(), crate::representation::OutputError> {
                let _ = serde_json::to_writer(&mut target.0, &self$(.$val)?).unwrap();
                Ok(())
            }
        }
    }
}

pub struct OutputError {
    error_type: OutputErrorType,
}

enum OutputErrorType {
    Other,
}
