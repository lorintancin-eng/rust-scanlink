pub mod decoder;
pub mod deshred;
pub mod failover;
pub mod feed;
pub mod geyser;

pub use decoder::{NewToken, PumpBuyEvent, ScannerEvent};

pub const PUMP_PROGRAM_ID: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
pub const DISC_CREATE: [u8; 8] = [0x18, 0x1e, 0xc8, 0x28, 0x05, 0x1c, 0x07, 0x77];
pub const DISC_CREATE_V2: [u8; 8] = [214, 144, 76, 236, 95, 139, 49, 180];
pub const DISC_BUY: [u8; 8] = [102, 6, 61, 18, 1, 218, 235, 234];
