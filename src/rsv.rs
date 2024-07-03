use std::borrow::Cow;

pub fn decode_rsv(bytes: &[u8]) -> Vec<Cow<'_, str>> {
    bytes
        .split(|b| b == &0xFF)
        .filter_map(|s| {
            if s.is_empty() {
                return None;
            }
            Some(String::from_utf8_lossy(s))
        })
        .collect()
}

fn encode_rsv(input: &[&str]) -> Vec<u8> {
    let mut ret = Vec::new();
    for word in input {
        ret.extend(word.as_bytes());
        ret.push(0xFF);
    }

    ret
}

#[test]
fn decode_rsv_test() {
    let word = "asdf".as_bytes();

    let mut test: Vec<u8> = Vec::new();
    for _ in 0..4 {
        test.extend(word);
        test.extend(&[0xFF]);
    }

    eprintln!("{:#?}", decode_rsv(&test));
}
