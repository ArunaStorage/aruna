use crate::{
    constants::{Field, FIELDS},
    error::ArunaError,
};
use heed::RwTxn;
use milli::{FieldsIdsMap, Index};

pub(crate) fn prepopulate_fields(
    index: &Index,
    mut wtxn: &mut RwTxn<'_>,
) -> Result<(), ArunaError> {
    let mut field_ids_map = FieldsIdsMap::default();
    for (idx, Field { name, index }) in FIELDS.iter().enumerate() {
        let field_map_index = field_ids_map
            .insert(*name)
            .ok_or_else(|| ArunaError::ServerError(format!("Unable to pre-populate field")))?;
        assert_eq!(idx, *index as usize);
        assert_eq!(field_map_index, *index as u16);
    }

    // Ensure that the existing map has the expected field u32 mappings
    let existing_map = index.fields_ids_map(&wtxn)?;

    if !existing_map.is_empty() {
        existing_map.iter().zip(field_ids_map.iter()).for_each(
            |((got_id, got_name), (id, name))| {
                assert_eq!(got_id, id);
                assert_eq!(got_name, name);
            },
        );
    } else {
        index.put_fields_ids_map(&mut wtxn, &field_ids_map)?;
    }
    Ok(())
}
