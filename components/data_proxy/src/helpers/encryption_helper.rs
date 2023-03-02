const ENCRYPTION_BLOCK_SIZE: usize = 65_536;

pub fn calculate_padding(size: usize) -> usize {
    let remainder = size % ENCRYPTION_BLOCK_SIZE;

    if remainder == 0 {
        0
    } else {
        // The minimum padding size is 8 bytes, so if the remainder plus minimum padding is larger than the blocksize
        // -> Add a full 64kB block
        // else return the missing bytes to the next "full" block
        if remainder + 8 > ENCRYPTION_BLOCK_SIZE {
            (ENCRYPTION_BLOCK_SIZE - remainder) + ENCRYPTION_BLOCK_SIZE
        } else {
            ENCRYPTION_BLOCK_SIZE - remainder
        }
    }
}
