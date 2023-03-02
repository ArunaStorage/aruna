use anyhow::{anyhow, Result};
use byteorder::{LittleEndian, WriteBytesExt};

pub fn _create_skippable_frame(size: usize) -> Result<Vec<u8>> {
    if size < 8 {
        return Err(anyhow!("{size} is too small, minimum is 8 bytes"));
    }
    // Add frame_header
    let mut frame = hex::decode("512A4D18")?;
    // 4 Bytes (little-endian) for size
    frame.write_u32::<LittleEndian>(size as u32 - 8)?;
    frame.extend(vec![0; size - 8]);
    Ok(frame)
}
