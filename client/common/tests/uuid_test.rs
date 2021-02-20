use common::uuid;

#[test]
fn test_uuid_string()->Result<(), String> {
    let id = uuid::uuid_string();
    if id.is_empty(){
        return Err(format!("got empty string"));
    }
    println!("uuid string: {}", id);
    Ok(())
}

#[test]
fn test_uuid_u64()->Result<(), String>{
    let ids = uuid::uuid_u64_le();
    if ids.is_empty(){
        return Err(format!("got empty vec"));
    }
    println!("uuid: {:?}", ids);
    Ok(())
}