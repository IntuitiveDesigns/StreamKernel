package streamkernel.authz

# Default Deny (Strict)
default allow = false

allowed_topics = {
    "tickets_vectorized",
    "arena-bench-test"
}

# The Real Rule
allow {
    input.user == "bench-user"
    lower(input.action) == "write"
    allowed_topics[input.resource]
}
