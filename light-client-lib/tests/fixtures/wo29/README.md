These pristine database archives were created by WO29's actual historical writer
and read by its separate candidate-reader binary. They are not records fabricated
by deleting metadata from the new implementation. They are copied byte-for-byte
from WO29/evidence/positional-release-physical-upgrades. Exact released-writer
source provenance remains unproven; these fixtures do not certify v0.5.4 or
v0.5.5-rc1 compatibility. Original archives and evidence remain untouched.

RocksDB SHA256: fcf1d192e890b474f630774e0188dcd1f5dd2c4cf9c55fa175d6a806d08ffcbd
SQLite SHA256: 1a95c27610aa2fa71e79d5d6ba046fcb4ff2a2d6cbe3078c405dac4b5342b65f

The explicit ignored canonical_status test upgrades a copy, verifies preservation
of primary bytes and user data, rebuilds the creator/spend range, and reopens it.
Run it using WO29_LEGACY_DB; the fork-only WO44 workflow supplies this path.
