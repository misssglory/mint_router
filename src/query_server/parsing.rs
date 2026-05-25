pub fn split_parts(input: &str) -> Vec<&str> {
    input.split_whitespace().collect()
}

pub fn join_tail(parts: &[&str], start: usize) -> String {
    parts.get(start..).unwrap_or(&[]).join(" ")
}

pub fn normalize_quoted(value: &str) -> String {
    value
        .trim()
        .trim_start_matches('"')
        .trim_end_matches('"')
        .to_string()
}