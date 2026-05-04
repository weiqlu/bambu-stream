use serde_json::Value;

pub fn deep_merge(base: &mut Value, delta: Value) {
    match (base, delta) {
        (Value::Object(b), Value::Object(d)) => {
            for (k, v) in d {
                deep_merge(b.entry(k).or_insert(Value::Null), v);
            }
        }
        (b, d) => *b = d,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn merges_nested_objects() {
        let mut base = json!({"a": {"x": 1, "y": 2}});
        deep_merge(&mut base, json!({"a": {"y": 20, "z": 30}}));
        assert_eq!(base, json!({"a": {"x": 1, "y": 20, "z": 30}}));
    }

    #[test]
    fn replaces_arrays_wholesale() {
        let mut base = json!({"ams": [{"id": 0}, {"id": 1}]});
        deep_merge(&mut base, json!({"ams": [{"id": 2}]}));
        assert_eq!(base, json!({"ams": [{"id": 2}]}));
    }

    #[test]
    fn replaces_scalars() {
        let mut base = json!({"temp": 25.0});
        deep_merge(&mut base, json!({"temp": 26.5}));
        assert_eq!(base, json!({"temp": 26.5}));
    }

    #[test]
    fn inserts_new_keys() {
        let mut base = json!({});
        deep_merge(&mut base, json!({"new": "value"}));
        assert_eq!(base, json!({"new": "value"}));
    }
}
