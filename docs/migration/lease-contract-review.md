# Lease review

The review identified two authority failures in the baseline.
The database accepted caller-supplied time and used a runtime ID as its only owner token.
Old Pi processes retained writable SQLite handles after package replacement.

The selected contract uses a fresh random grant for each connection.
The daemon supplies time and checks both connection ownership and database ownership.
Disconnect, expiry, release, and restart invalidate the grant.
An asynchronous Pi callback cannot adopt a replacement grant.

The migration preserves the database inode.
A single transaction renames durable tables and installs empty read-only views under their old names.
Those views prevent legacy constructors from recreating writer tables.
Old writes must either commit before the migration or fail afterward.

The review proposed several explicit epoch fields.
The selected implementation uses a random connection token instead.
That token changes across restarts and suffices for the local fencing comparison.
The filesystem owner remains the trust boundary.
Tokens do not isolate mutually untrusted processes under the same user.

The deciding tests use actual legacy code with an open database handle.
Prepared writes, old constructors, and delayed receipt callbacks need separate checks.
These review conclusions are design inputs, not deployment evidence.
