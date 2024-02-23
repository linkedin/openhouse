package openhouse.authorization

import data.user_roles
import data.privileges

# Not authorized by default
default allow := false

allow {
  # Get requested resource
  current_db := data.user_roles[input.db_id]
  current_tbl := current_db[input.tbl_id]

  # Get roles mapped to user on resource
  current_user_roles_on_resource := current_tbl[input.user]
  role := current_user_roles_on_resource.roles[_]

  # Get privileges mapped to each role that the user has
  role_privileges := data.privileges[role]
  privilege := role_privileges[_]

  # Check if privileges that the user has matches the privilege requested
  privilege == input.privilege_to_check
}

allow {
  # Get requested resource
  current_db := data.user_roles[input.db_id]

  # Get roles mapped to user on resource
  current_user_roles_on_resource := current_db[input.user]
  role := current_user_roles_on_resource.roles[_]

  # Get privileges mapped to each role that the user has
  role_privileges := data.privileges[role]
  privilege := role_privileges[_]

  # Check if privileges that the user has matches the privilege requested
  privilege == input.privilege_to_check
}

# OpenHouse is granted all access
allow {
  input.user == "openhouse"
}
