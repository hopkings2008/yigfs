use common::uuid;
use common::numbers::NumberOp;
use log::info;

#[test]
fn test_uuid_string()->Result<(), String> {
    let id = uuid::uuid_string();
    if id.is_empty(){
        return Err(format!("got empty string"));
    }
    info!("uuid string: {}", id);
    Ok(())
}

#[test]
fn test_uuid_u64()->Result<(), String>{
    let ids = uuid::uuid_u64_le();
    if ids.is_empty(){
        return Err(format!("got empty vec"));
    }
    info!("uuid: {:?}", ids);
    Ok(())
}

#[test]
fn test_numberop_uuid() -> Result<(), String>{
    let ids = uuid::uuid_u64_le();
    let id = NumberOp::to_u128(ids[0], ids[1]);
    let vids = NumberOp::from_u128(id);
    if ids[0] != vids[0] || ids[1] != vids[1] {
        info!("original ids: {:?}, converted ids: {:?}", ids, vids);
        return Err(format!("original ids: {:?}, converted ids: {:?}", ids, vids));
    }
    Ok(())
}