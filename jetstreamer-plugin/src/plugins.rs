/// Plugin that records total instructions per slot.
#[cfg(feature = "clickhouse")]
pub mod instruction_tracking;
/// Default plugin that aggregates program invocation statistics.
#[cfg(feature = "clickhouse")]
pub mod program_tracking;
/// Plugin that prints JSON Lines block records with shredding metadata to stdout.
pub mod shred_dump;
