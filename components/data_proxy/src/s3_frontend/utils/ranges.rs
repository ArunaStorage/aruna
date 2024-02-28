use anyhow::anyhow;
use anyhow::Result;
use pithos_lib::helpers::footer_parser::Footer;
use pithos_lib::helpers::structs::Range as ArunaRange;
use pithos_lib::pithos::structs::FileContextVariants;
use s3s::dto::Range as S3Range;
use s3s::dto::Range::{Int, Suffix};

use crate::structs::ObjectLocation;

#[tracing::instrument(level = "trace", skip(input_range, content_length, footer))]
pub fn calculate_ranges(
    input_range: Option<S3Range>,
    content_length: u64,
    footer: Option<Footer>,
    location: &ObjectLocation,
) -> Result<(Option<String>, Option<Vec<u64>>, u64)> {
    let Some(range) = input_range else {
        return Ok((None, None, content_length));
    };
    let aruna_range = aruna_range_from_s3range(range, content_length);
    if location.is_pithos() {
        let Some(footer) = footer else {
            return Err(anyhow!("Footer not found"));
        };
        let file = footer
            .table_of_contents
            .files
            .first()
            .ok_or_else(|| anyhow!("No files in footer"))?;
        let FileContextVariants::FileDecrypted(file) = file else {
            return Err(anyhow!("File not decrypted"));
        };
        let (a, b) = file.get_range_and_filter_by_range(pithos_lib::helpers::structs::Range {
            from: aruna_range.from as u64,
            to: aruna_range.to as u64,
        });
        return Ok((
            Some(format!("bytes={}-{}", a.from, a.to)),
            Some(b),
            calculate_content_length_from_range(a),
        ));
    }

    if location.is_compressed() || location.get_encryption_key().is_some() {
        return Ok((
            None,
            Some(vec![aruna_range.from, aruna_range.to]),
            aruna_range.to - aruna_range.from,
        ));
    }

    return Ok((
        Some(format!("bytes={}-{}", aruna_range.from, aruna_range.to)),
        None,
        aruna_range.to - aruna_range.from,
    ));
}

#[tracing::instrument(level = "trace", skip(range))]
pub fn calculate_content_length_from_range(range: pithos_lib::helpers::structs::Range) -> u64 {
    range.to - range.from // Note: -1 bytes-ranges are inclusive
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
