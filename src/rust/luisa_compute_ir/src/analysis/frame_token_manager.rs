use std::collections::HashSet;
use std::ops::Deref;
use lazy_static::lazy_static;

pub(crate) struct FrameTokenManager {
    frame_token_counter: u32,
    frame_token_occupied: HashSet<u32>,
}

pub(crate) static INVALID_FRAME_TOKEN_MASK: u32 = 0x8000_0000;

impl FrameTokenManager {
    pub(crate) fn register_frame_token(token: u32) {
        let ftm = Self::get_instance();
        assert_eq!(token & INVALID_FRAME_TOKEN_MASK, 0, "Invalid frame token");
        assert!(!ftm.frame_token_occupied.contains(&token),
                "Frame token already occupied");
        ftm.frame_token_occupied.insert(token);
    }
    pub(crate) fn get_new_token() -> u32 {
        let ftm = Self::get_instance();
        while ftm.frame_token_occupied.contains(&ftm.frame_token_counter) {
            assert_ne!(ftm.frame_token_counter, INVALID_FRAME_TOKEN_MASK, "Frame token overflow");
            ftm.frame_token_counter += 1;
        }
        ftm.frame_token_occupied.insert(ftm.frame_token_counter);
        ftm.frame_token_counter
    }

    pub(crate) fn get_instance() -> &'static mut Self {
        lazy_static!(
            static ref INSTANCE: FrameTokenManager = FrameTokenManager {
                frame_token_counter: 0,
                frame_token_occupied: HashSet::new(),
            };
        );
        let p: *const FrameTokenManager = INSTANCE.deref();
        unsafe {
            let p: *mut FrameTokenManager = std::mem::transmute(p);
            p.as_mut().unwrap()
        }
    }

    pub(crate) fn reset() {
        let ftm = Self::get_instance();
        ftm.frame_token_counter = 0;
        ftm.frame_token_occupied.clear();
    }
}