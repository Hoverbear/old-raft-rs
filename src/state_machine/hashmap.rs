#![cfg(feature="hashmap")]
use std::io::{Error, Result};
use std::collections::HashMap;

use serde::json::{self, Value};

use state_machine::StateMachine;

/// A state machine that holds a single mutable value.
#[derive(Debug)]
pub struct HashmapStateMachine {
    map: HashMap<String, Value>,
}

/// Generally maps (key, value)
#[derive(Serialize, Deserialize)]
pub struct Message(String, Option<Value>);

impl HashmapStateMachine {
    pub fn new() -> HashmapStateMachine {
        HashmapStateMachine {
            map: HashMap::new(),
        }
    }
}

impl StateMachine for HashmapStateMachine {

    type Error = Error;

    fn apply(&mut self, new_value: &[u8]) -> Result<Vec<u8>> {
        let string = String::from_utf8_lossy(new_value);
        let Message(key, value) = json::from_str(&string).unwrap();

        let response = json::to_string(&match value {
            Some(v) => (key.clone(), self.map.insert(key, v)),
            None => (key.clone(), self.map.remove(&key)),
        }).unwrap();

        Ok(response.into_bytes())
    }

    fn query(&self, query: &[u8]) -> Result<Vec<u8>> {
        let string = String::from_utf8_lossy(query);
        let Message(key, _) = json::from_str(&string).unwrap();

        let response = json::to_string(&self.map.get(&key).map(|v| v.clone())).unwrap();

        Ok(response.into_bytes())
    }

    fn snapshot(&self) -> Result<Vec<u8>> {
        Ok(json::to_string(&self.map)
            .unwrap()
            .into_bytes())
    }

    fn restore_snapshot(&mut self, snapshot_value: Vec<u8>) -> Result<()> {
        self.map = json::from_str(&String::from_utf8_lossy(&snapshot_value)).unwrap();
        Ok(())
    }
}
