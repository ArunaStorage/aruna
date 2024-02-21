use anyhow::Result;
use aruna_file::helpers::footer_parser::{FooterParser, Range as ArunaRange};
use s3s::dto::Range as S3Range;
use s3s::dto::Range::{Int, Suffix};

#[tracing::instrument(level = "trace", skip(input_range, content_length, footer))]
pub fn calculate_ranges(
    input_range: Option<S3Range>,
    content_length: u64,
    footer: Option<FooterParser>,
) -> Result<(Option<String>, Option<ArunaRange>, u64)> {
    todo!()
}

#[tracing::instrument(level = "trace", skip(range))]
pub fn calculate_content_length_from_range(range: ArunaRange) -> i64 {
    (range.to - range.from) as i64 // Note: -1 bytes-ranges are inclusive
}

#[tracing::instrument(level = "trace", skip(range_string, content_length))]
pub fn aruna_range_from_s3range(range_string: S3Range, content_length: u64) -> ArunaRange {
    match range_string {
        Int { first, last } => match last {
            Some(val) => ArunaRange {
                from: first,
                to: if val > content_length {
                    content_length - 1
                } else {
                    val
                },
            },
            None => ArunaRange {
                from: first,
                to: content_length,
            },
        },
        Suffix { length } => ArunaRange {
            from: content_length - length,
            to: content_length,
        },
    }
}
