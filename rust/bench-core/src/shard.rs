pub fn bot_to_symbol(bot_index: usize, num_symbols: usize) -> usize {
    if num_symbols == 0 {
        return 0;
    }
    bot_index % num_symbols
}

pub fn symbol_name(idx: usize) -> String {
    format!("SYM_{:04}", idx)
}

pub fn symbol_shard(symbol: &str, num_shards: usize) -> usize {
    if num_shards <= 1 {
        return 0;
    }
    let mut h: u64 = 14695981039346656037;
    for b in symbol.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(1099511628211);
    }
    (h as usize) % num_shards
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_deterministically() {
        assert_eq!(bot_to_symbol(0, 4), 0);
        assert_eq!(bot_to_symbol(5, 4), 1);
        assert_eq!(bot_to_symbol(123, 1), 0);
        assert_eq!(bot_to_symbol(7, 0), 0);
    }

    #[test]
    fn symbol_shard_is_stable() {
        let a = symbol_shard("SYM_0001", 8);
        let b = symbol_shard("SYM_0001", 8);
        assert_eq!(a, b);
        assert!(a < 8);
    }
}
