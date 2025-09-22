//! Typed output buffers for several representations (JSON, CLI, HTML)

pub trait GenOutput<T> {
    fn write(&self, target: &mut T) -> Result<(), OutputError>;

    fn write_seq_start(_target: &mut T) -> Result<(), OutputError> {
        Ok(())
    }

    fn write_seq_end(_target: &mut T) -> Result<(), OutputError> {
        Ok(())
    }

    fn write_seq_sep(_target: &mut T) -> Result<(), OutputError> {
        Ok(())
    }
}

// would be nice to do something like this ..
//impl<'a, T, G: 'a + GenOutput<T>, I: Iterator<Item=&'a G>> GenOutput<T> for &'a mut I {
//    fn write(&mut self, target: &mut T) -> Result<(), OutputError> {
//        G::write_seq_start(target)?;
//        for i in self.next() {
//            i.write(target)?;
//        }
//        G::write_seq_end(target)
//    }
//}

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
            fn write_seq_start(target: &mut Json<W>) -> Result<(), crate::representation::OutputError> {
                let _ = write!(&mut target.0, "[");
                Ok(())
            }
            fn write_seq_end(target: &mut Json<W>) -> Result<(), crate::representation::OutputError> {
                let _ = write!(&mut target.0, "]");
                Ok(())
            }
            fn write_seq_sep(target: &mut Json<W>) -> Result<(), crate::representation::OutputError> {
                let _ = write!(&mut target.0, ",");
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
