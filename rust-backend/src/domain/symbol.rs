use super::Exchange;

const MAX_SYMBOL_CHARACTERS: usize = 32;
const MAX_SYMBOL_BYTES: usize = 128;

pub(crate) fn is_valid_symbol_text(value: &str) -> bool {
    !value.is_empty()
        && value == value.trim()
        && value.chars().count() <= MAX_SYMBOL_CHARACTERS
        && value.len() <= MAX_SYMBOL_BYTES
        && value.chars().all(|character| {
            character.is_ascii_uppercase()
                || character.is_ascii_digit()
                || (!character.is_ascii() && character.is_alphanumeric())
        })
}

pub(crate) fn is_valid_symbol_for_exchange(exchange: Exchange, value: &str) -> bool {
    is_valid_symbol_text(value) && (exchange == Exchange::Aster || value.is_ascii())
}

pub(crate) fn normalize_symbol_for_exchange(exchange: Exchange, value: &str) -> Option<String> {
    let normalized = value.trim().to_ascii_uppercase();
    is_valid_symbol_for_exchange(exchange, &normalized).then_some(normalized)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unicode_symbols_are_scoped_to_aster() {
        assert_eq!(
            normalize_symbol_for_exchange(Exchange::Aster, " 牛来usdt "),
            Some("牛来USDT".into())
        );
        assert_eq!(
            normalize_symbol_for_exchange(Exchange::Binance, "牛来USDT"),
            None
        );
        assert_eq!(
            normalize_symbol_for_exchange(Exchange::Binance, "muusdt"),
            Some("MUUSDT".into())
        );
    }
}
