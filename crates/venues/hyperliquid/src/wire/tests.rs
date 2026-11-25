use super::*;

fn px(text: &str) -> Result<Px, serde_json::Error> {
    serde_json::from_str(&format!("\"{text}\""))
}

fn qty(text: &str) -> Result<Qty, serde_json::Error> {
    serde_json::from_str(&format!("\"{text}\""))
}

#[test]
fn price_lands_on_the_nanosecond_grid() {
    assert_eq!(px("59768.0").unwrap().0, 59_768_000_000_000);
    assert_eq!(px("0.000000001").unwrap().0, 1);
}

#[test]
fn price_outside_the_envelope_is_an_error() {
    // above the 9.22e9 envelope, finer than the 1e-9 tick, negative, not a decimal
    for bad in ["9300000000.0", "0.0000000001", "-1.0", "1e5", "", "."] {
        assert!(px(bad).is_err(), "{bad}");
    }
}

#[test]
fn size_is_exact_at_eight_places_and_never_negative() {
    assert_eq!(qty("38158.90624").unwrap().0, 3_815_890_624_000);
    assert!(qty("0.000000001").is_err());
    assert!(qty("-0.5").is_err());
    assert!(qty("184467440738.09551616").is_err());
}
