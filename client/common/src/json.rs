use serde_json;
use serde::{Serialize, Deserialize};

pub fn encode_to_str<'a, T: Default + Serialize> (p: &'a T) -> Result<String, String> {
    let ret = serde_json::to_string(p);
    match ret {
        Ok(body) => {
            return Ok(body);
        }
        Err(error) => {
            return Err(format!("encode_to_str failed with err: {}", error));
        }
    }
}

pub fn decode_from_str<'a, T: Default+Deserialize<'a>> (body: &'a String) -> Result<T, String>{
    let mut err = String::new();
    let p : T = match serde_json::from_str(body){
        Ok(p) => {
            p
        }
        Err(error) => {
            err = format!("{}", error);
            Default::default()
        }
    };

    if err != ""{
        return Err(err);
    }

    return Ok(p);
}
