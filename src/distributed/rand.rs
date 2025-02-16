use std::cmp::max;

pub(crate) fn get_timer_reset(id: u64) -> i64 {
    // some randomness on the timer would help avoid constant elections
    max(5, (max(id, 3000) / 100) - 30) as i64
}

#[cfg(test)]
mod tests {
    use crate::distributed::rand::get_timer_reset;

    #[test]
    fn test_get_random() {
        let r4000 = get_timer_reset(4000);
        let r5000 = get_timer_reset(5000);
        let r6000 = get_timer_reset(6000);
        assert!(r4000 > 0);
        assert!(r5000 > 0);
        assert!(r6000 > 0);

        assert!(r4000 + 5 < r5000);
        assert!(r5000 + 5 < r6000);
    }
}
