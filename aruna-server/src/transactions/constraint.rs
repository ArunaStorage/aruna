use super::viewstore::ViewStore;
use diesel_ulid::DieselUlid;
use thiserror::Error;

pub enum Constraint {
    ParentExists(DieselUlid),
    UniqueName { parent_id: DieselUlid, name: String },
    //UniqueGroup { realm: DieselUlid, name: String },
}

impl ViewStore {
    pub fn check_constraints(&self, constraints: Vec<Constraint>) -> Result<(), ConstraintError> {
        for constraint in constraints {
            self.check_constraint(constraint)?;
        }
        Ok(())
    }

    fn check_constraint(&self, constraint: Constraint) -> Result<(), ConstraintError> {
        match constraint {
            Constraint::ParentExists(parent_id) => self.check_constraint_parent_exists(parent_id),
            Constraint::UniqueName {
                parent_id,
                ref name,
            } => self.check_constraint_unique_name(parent_id, name),
            //Constraint::UniqueGroup { realm, ref name } => {
            //    self.check_constraint_unique_group(realm, name)
            //}
        }
    }

    fn check_constraint_parent_exists(&self, parent_id: DieselUlid) -> Result<(), ConstraintError> {
        self.node_idx
            .get(&parent_id)
            .map_or(Err(ConstraintError::ParentDoesNotExist), |_| Ok(()))
    }

    fn check_constraint_unique_name(
        &self,
        parent_id: DieselUlid,
        name: &str,
    ) -> Result<(), ConstraintError> {
        // Checks if project name is (for now) uniqe
        if self.groups.get(&parent_id).is_some() {
            for (_, res) in self.resources.iter() {
                if matches!(
                    res.variant(),
                    crate::api::models::aruna::ResourceVariant::Project
                ) {
                    if res.name == name {
                        return Err(ConstraintError::NameNotUnique(res.name.clone()));
                    }
                }
            }
        } else {
            // Checks if name is already taken in hierarchy
            for child in self.get_children(&parent_id) {
                let Some(child_res) = self.resources.get(&child) else {
                    panic!("Child not found");
                };
                if child_res.name == name {
                    return Err(ConstraintError::NameNotUnique(child_res.name.to_string()));
                }
            }
        }
        Ok(())
    }
}
