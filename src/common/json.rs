//------------ EasilyExtendedJSONObject --------------------------------------

use serde_json::Value;

pub trait EasilyExtendedJSONObject {
    fn insert(&mut self, key: &str, value: Value) -> Option<Value>;

    fn push(&mut self, value: Value);
}

impl EasilyExtendedJSONObject for Value {
    /// Assumes that the given `is_object()` is true for the given `Value`.
    fn insert(&mut self, key: &str, value: Value) -> Option<Value> {
        self.as_object_mut().unwrap().insert(key.to_string(), value)
    }

    fn push(&mut self, value: Value) {
        self.as_array_mut().unwrap().push(value)
    }
}
