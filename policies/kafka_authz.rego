package streamkernel.authz

import rego.v1

default allow = false

allowed_topics := {
  "tickets_vectorized",
  "arena-bench-test"
}

allow if {
    input.user == "service-account-1"
    input.action == "write"
    input.resource in allowed_topics
}