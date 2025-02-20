package openhouse.authorization

import rego.v1

import future.keywords.in

# Not authorized by default
default allow := false

allow if {
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

allow if {
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

# Allow any user to read if a global read role is granted on the table
allow if {
	current_db := data.user_roles[input.db_id]
	current_tbl := current_db[input.tbl_id]

	# Check if the global users have permission on the table
	global_user_roles_on_resource := current_tbl["*"]
  role := global_user_roles_on_resource.roles[_]

  # Ensure that global users only have the table viewer permission
  role == "TABLE_VIEWER"
  role_privileges := data.privileges[role]

  # Validate that the user is only asking for table viewing privileges
  input.privilege_to_check in role_privileges
}

# Granting all access for default user
allow if {
	input.user == "DUMMY_ANONYMOUS_USER"
}

# openhouse is granted all access
allow if {
	input.user == "openhouse"
}
