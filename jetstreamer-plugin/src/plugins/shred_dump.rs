use std::{collections::HashMap, sync::Arc};

use clickhouse::Client;
use dashmap::DashMap;
use futures_util::FutureExt;
use jetstreamer_firehose::{
    block::Shredding,
    firehose::{BlockData, EntryData, TransactionData},
};
use once_cell::sync::Lazy;
use serde::Serialize;
use solana_entry::entry::Entry as SolanaEntry;
use solana_hash::Hash as SolanaHash;
use solana_keypair::Keypair;
use solana_ledger::shred::{
    ProcessShredsStats, ReedSolomonCache, SIZE_OF_DATA_SHRED_HEADERS, Shredder,
};
use solana_transaction::versioned::VersionedTransaction;

use crate::{Plugin, PluginFuture};

static PENDING_BY_SLOT: Lazy<DashMap<u64, PendingSlotDump>> = Lazy::new(DashMap::new);

#[derive(Debug, Clone)]
/// Prints JSON Lines block records with merged shredding, entry, and transaction metadata.
pub struct ShredDumpPlugin;

impl ShredDumpPlugin {
    /// Creates a new stdout shred dump plugin.
    pub const fn new() -> Self {
        Self
    }

    fn take_pending_slot(slot: u64) -> PendingSlotDump {
        PENDING_BY_SLOT
            .remove(&slot)
            .map(|(_, pending)| pending)
            .unwrap_or_default()
    }

    fn clear_pending_slot(slot: u64) {
        PENDING_BY_SLOT.remove(&slot);
    }

    fn clear_all_pending() {
        let slots: Vec<u64> = PENDING_BY_SLOT.iter().map(|entry| *entry.key()).collect();
        for slot in slots {
            PENDING_BY_SLOT.remove(&slot);
        }
    }
}

impl Default for ShredDumpPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl Plugin for ShredDumpPlugin {
    #[inline(always)]
    fn name(&self) -> &'static str {
        "Shred Dump"
    }

    #[inline(always)]
    fn on_transaction<'a>(
        &'a self,
        _thread_id: usize,
        _db: Option<Arc<Client>>,
        transaction: &'a TransactionData,
    ) -> PluginFuture<'a> {
        async move {
            let mut slot_dump = PENDING_BY_SLOT.entry(transaction.slot).or_default();
            slot_dump.transactions.insert(
                transaction.transaction_slot_index,
                PendingTransaction {
                    transaction_slot_index: transaction.transaction_slot_index,
                    signature: transaction.signature.to_string(),
                    is_vote: transaction.is_vote,
                    transaction: transaction.transaction.clone(),
                },
            );
            Ok(())
        }
        .boxed()
    }

    #[inline(always)]
    fn on_entry<'a>(
        &'a self,
        _thread_id: usize,
        _db: Option<Arc<Client>>,
        entry: &'a EntryData,
    ) -> PluginFuture<'a> {
        async move {
            let mut slot_dump = PENDING_BY_SLOT.entry(entry.slot).or_default();
            slot_dump.entries.insert(
                entry.entry_index,
                PendingEntry {
                    entry_index: entry.entry_index,
                    transaction_start_idx: entry.transaction_indexes.start,
                    transaction_end_idx_exclusive: entry.transaction_indexes.end,
                    num_hashes: entry.num_hashes,
                    hash: entry.hash,
                },
            );
            Ok(())
        }
        .boxed()
    }

    #[inline(always)]
    fn on_block<'a>(
        &'a self,
        thread_id: usize,
        _db: Option<Arc<Client>>,
        block: &'a BlockData,
    ) -> PluginFuture<'a> {
        async move {
            let pending = Self::take_pending_slot(block.slot());
            print_block_record(thread_id, block, pending);
            Ok(())
        }
        .boxed()
    }

    #[inline(always)]
    fn on_exit(&self, _db: Option<Arc<Client>>) -> PluginFuture<'_> {
        async move {
            Self::clear_all_pending();
            Ok(())
        }
        .boxed()
    }
}

#[derive(Default)]
struct PendingSlotDump {
    entries: HashMap<usize, PendingEntry>,
    transactions: HashMap<usize, PendingTransaction>,
}

#[derive(Clone)]
struct PendingEntry {
    entry_index: usize,
    transaction_start_idx: usize,
    transaction_end_idx_exclusive: usize,
    num_hashes: u64,
    hash: SolanaHash,
}

#[derive(Clone)]
struct PendingTransaction {
    transaction_slot_index: usize,
    signature: String,
    is_vote: bool,
    transaction: VersionedTransaction,
}

#[derive(Serialize)]
struct BlockDumpRecord {
    event: &'static str,
    thread_id: usize,
    slot: u64,
    parent_slot: u64,
    blockhash: String,
    parent_blockhash: String,
    block_time: Option<i64>,
    block_height: Option<u64>,
    executed_transaction_count: u64,
    entry_count: u64,
    metadata: BlockDumpMetadata,
}

#[derive(Serialize)]
struct BlockDumpMetadata {
    shredding_count: usize,
    shredding: Vec<ShredDumpEntry>,
    merged_entry_count: usize,
    merged_transaction_count: usize,
    reconstructed_shred_count: Option<usize>,
    raw_shred_count_estimate: Option<i64>,
    reconstruction_error: Option<String>,
    entries: Vec<EntryDumpEntry>,
    transactions: Vec<TransactionDumpEntry>,
}

#[derive(Serialize, Clone)]
struct ShredDumpEntry {
    entry_end_idx: i64,
    shred_end_idx: i64,
}

#[derive(Serialize)]
struct EntryDumpEntry {
    entry_index: usize,
    transaction_start_idx: usize,
    transaction_end_idx_exclusive: usize,
    transaction_count: usize,
    num_hashes: u64,
    hash: String,
    shredding: Option<ShredDumpEntry>,
    serialized_offset_start: Option<usize>,
    serialized_offset_end_exclusive: Option<usize>,
    serialized_size: Option<usize>,
    start_shred_idx: Option<u32>,
    end_shred_idx: Option<u32>,
    start_offset_in_shred: Option<usize>,
    end_offset_in_shred_exclusive: Option<usize>,
    reconstructed_shred_end_idx: Option<i64>,
    raw_shred_end_idx_matches_reconstruction: Option<bool>,
}

#[derive(Serialize)]
struct TransactionDumpEntry {
    transaction_slot_index: usize,
    signature: String,
    is_vote: bool,
    entry_index: Option<usize>,
    serialized_offset_start: Option<usize>,
    serialized_offset_end_exclusive: Option<usize>,
    serialized_size: Option<usize>,
    start_shred_idx: Option<u32>,
    end_shred_idx: Option<u32>,
    start_offset_in_shred: Option<usize>,
    end_offset_in_shred_exclusive: Option<usize>,
}

#[derive(Serialize)]
struct SkippedBlockRecord {
    event: &'static str,
    thread_id: usize,
    slot: u64,
}

struct Reconstruction {
    reconstructed_shred_count: usize,
    entry_placements: HashMap<usize, EntryPlacement>,
    transaction_placements: HashMap<usize, TransactionPlacement>,
}

struct EntryPlacement {
    serialized_offset_start: usize,
    serialized_offset_end_exclusive: usize,
    start_shred_idx: u32,
    end_shred_idx: u32,
    start_offset_in_shred: usize,
    end_offset_in_shred_exclusive: usize,
    reconstructed_shred_end_idx: i64,
}

struct TransactionPlacement {
    entry_index: usize,
    serialized_offset_start: usize,
    serialized_offset_end_exclusive: usize,
    start_shred_idx: u32,
    end_shred_idx: u32,
    start_offset_in_shred: usize,
    end_offset_in_shred_exclusive: usize,
}

struct RebuildableEntry {
    entry_index: usize,
    transaction_slot_indexes: Vec<usize>,
    entry_serialized_len: usize,
    entry_prefix_len: usize,
    transaction_serialized_lens: Vec<usize>,
}

struct ShredByteRange {
    shred_index: u32,
    serialized_offset_start: usize,
    serialized_offset_end_exclusive: usize,
}

struct BytePlacement {
    start_shred_idx: u32,
    end_shred_idx: u32,
    start_offset_in_shred: usize,
    end_offset_in_shred_exclusive: usize,
}

fn print_block_record(thread_id: usize, block: &BlockData, pending: PendingSlotDump) {
    let serialized = match block {
        BlockData::Block {
            parent_slot,
            parent_blockhash,
            slot,
            blockhash,
            block_time,
            block_height,
            executed_transaction_count,
            entry_count,
            shredding,
            ..
        } => format_block_record(
            thread_id,
            *slot,
            *parent_slot,
            &blockhash.to_string(),
            &parent_blockhash.to_string(),
            *block_time,
            *block_height,
            *executed_transaction_count,
            *entry_count,
            shredding,
            &pending.entries,
            &pending.transactions,
        ),
        BlockData::PossibleLeaderSkipped { slot } => {
            ShredDumpPlugin::clear_pending_slot(*slot);
            format_skipped_block_record(thread_id, *slot)
        }
    };

    match serialized {
        Ok(line) => println!("{line}"),
        Err(err) => eprintln!("failed to serialize shred dump block record: {err}"),
    }
}

fn format_block_record(
    thread_id: usize,
    slot: u64,
    parent_slot: u64,
    blockhash: &str,
    parent_blockhash: &str,
    block_time: Option<i64>,
    block_height: Option<u64>,
    executed_transaction_count: u64,
    entry_count: u64,
    shredding: &[Shredding],
    pending_entries: &HashMap<usize, PendingEntry>,
    pending_transactions: &HashMap<usize, PendingTransaction>,
) -> Result<String, serde_json::Error> {
    let resolved_shredding = resolve_shredding(shredding);
    let shredding_by_entry_end_idx = resolved_shredding
        .iter()
        .cloned()
        .map(|entry| (entry.entry_end_idx, entry))
        .collect::<HashMap<_, _>>();

    let reconstruction =
        match reconstruct_slot_layout(slot, parent_slot, pending_entries, pending_transactions) {
            Ok(layout) => (Some(layout), None),
            Err(err) => (None, Some(err)),
        };
    let reconstructed_shred_count = reconstruction
        .0
        .as_ref()
        .map(|layout| layout.reconstructed_shred_count);
    let entry_placements = reconstruction
        .0
        .as_ref()
        .map(|layout| &layout.entry_placements);
    let transaction_placements = reconstruction
        .0
        .as_ref()
        .map(|layout| &layout.transaction_placements);

    let entries = merge_entries(
        pending_entries,
        &shredding_by_entry_end_idx,
        entry_placements,
    );
    let transactions = merge_transactions(
        pending_transactions,
        &entries,
        transaction_placements,
        executed_transaction_count,
    );

    let record = BlockDumpRecord {
        event: "block",
        thread_id,
        slot,
        parent_slot,
        blockhash: blockhash.to_owned(),
        parent_blockhash: parent_blockhash.to_owned(),
        block_time,
        block_height,
        executed_transaction_count,
        entry_count,
        metadata: BlockDumpMetadata {
            shredding_count: resolved_shredding.len(),
            shredding: resolved_shredding,
            merged_entry_count: entries.len(),
            merged_transaction_count: transactions.len(),
            reconstructed_shred_count,
            raw_shred_count_estimate: raw_shred_count_estimate(shredding),
            reconstruction_error: reconstruction.1,
            entries,
            transactions,
        },
    };

    serde_json::to_string(&record)
}

fn format_skipped_block_record(thread_id: usize, slot: u64) -> Result<String, serde_json::Error> {
    serde_json::to_string(&SkippedBlockRecord {
        event: "possible_leader_skipped",
        thread_id,
        slot,
    })
}

fn resolve_shredding(shredding: &[Shredding]) -> Vec<ShredDumpEntry> {
    shredding
        .iter()
        .map(|shred| ShredDumpEntry {
            entry_end_idx: shred.entry_end_idx,
            shred_end_idx: shred.shred_end_idx,
        })
        .collect()
}

fn raw_shred_count_estimate(shredding: &[Shredding]) -> Option<i64> {
    shredding
        .iter()
        .filter_map(|shred| (shred.shred_end_idx >= 0).then_some(shred.shred_end_idx))
        .max()
        .map(|max_idx| max_idx + 1)
}

fn reconstruct_slot_layout(
    slot: u64,
    parent_slot: u64,
    pending_entries: &HashMap<usize, PendingEntry>,
    pending_transactions: &HashMap<usize, PendingTransaction>,
) -> Result<Reconstruction, String> {
    let mut sorted_entries = pending_entries.values().cloned().collect::<Vec<_>>();
    sorted_entries.sort_by_key(|entry| entry.entry_index);

    let mut slot_entries = Vec::with_capacity(sorted_entries.len());
    let mut rebuildable_entries = Vec::with_capacity(sorted_entries.len());

    for entry in sorted_entries {
        let transaction_slot_indexes =
            (entry.transaction_start_idx..entry.transaction_end_idx_exclusive).collect::<Vec<_>>();
        let mut transactions = Vec::with_capacity(transaction_slot_indexes.len());
        let mut transaction_serialized_lens = Vec::with_capacity(transaction_slot_indexes.len());

        for transaction_slot_index in &transaction_slot_indexes {
            let pending_transaction = pending_transactions
                .get(transaction_slot_index)
                .ok_or_else(|| {
                    format!(
                        "missing transaction {} needed by entry {}",
                        transaction_slot_index, entry.entry_index
                    )
                })?;
            let serialized_transaction_len =
                bincode::serialized_size(&pending_transaction.transaction).map_err(|err| {
                    format!(
                        "failed to serialize transaction {} for entry {}: {err}",
                        transaction_slot_index, entry.entry_index
                    )
                })? as usize;
            transaction_serialized_lens.push(serialized_transaction_len);
            transactions.push(pending_transaction.transaction.clone());
        }

        let solana_entry = SolanaEntry {
            num_hashes: entry.num_hashes,
            hash: entry.hash,
            transactions,
        };
        let serialized_entry_len = bincode::serialized_size(&solana_entry).map_err(|err| {
            format!(
                "failed to serialize entry {} while rebuilding shreds: {err}",
                entry.entry_index
            )
        })? as usize;
        let transactions_serialized_len = transaction_serialized_lens.iter().sum::<usize>();
        let entry_prefix_len = serialized_entry_len
            .checked_sub(transactions_serialized_len)
            .ok_or_else(|| {
                format!(
                    "serialized entry {} was shorter than the sum of its transactions",
                    entry.entry_index
                )
            })?;

        rebuildable_entries.push(RebuildableEntry {
            entry_index: entry.entry_index,
            transaction_slot_indexes,
            entry_serialized_len: serialized_entry_len,
            entry_prefix_len,
            transaction_serialized_lens,
        });
        slot_entries.push(solana_entry);
    }

    let keypair = Keypair::new();
    let shredder = Shredder::new(slot, parent_slot, 0, 0)
        .map_err(|err| format!("failed to initialize shredder for slot {slot}: {err}"))?;
    let reed_solomon_cache = ReedSolomonCache::default();
    let mut stats = ProcessShredsStats::default();
    let (mut data_shreds, _) = shredder.entries_to_merkle_shreds_for_tests(
        &keypair,
        &slot_entries,
        true,
        SolanaHash::default(),
        0,
        0,
        &reed_solomon_cache,
        &mut stats,
    );
    data_shreds.sort_by_key(|shred| shred.index());

    let deshredded = Shredder::deshred(data_shreds.iter().map(|shred| shred.payload().as_ref()))
        .map_err(|err| format!("failed to deshred reconstructed slot {slot}: {err}"))?;
    let entries_serialized_len = rebuildable_entries
        .iter()
        .map(|entry| entry.entry_serialized_len)
        .sum::<usize>();
    let outer_prefix_len = deshredded
        .len()
        .checked_sub(entries_serialized_len)
        .ok_or_else(|| {
            format!(
                "reconstructed slot {slot} serialized bytes were shorter than summed entry bytes"
            )
        })?;
    let shred_ranges = build_shred_ranges(&data_shreds, deshredded.len())?;

    let mut entry_placements = HashMap::with_capacity(rebuildable_entries.len());
    let mut transaction_placements = HashMap::new();
    let mut next_serialized_offset = outer_prefix_len;

    for entry in rebuildable_entries {
        let entry_start = next_serialized_offset;
        let entry_end = entry_start + entry.entry_serialized_len;
        let entry_byte_placement = locate_byte_placement(&shred_ranges, entry_start, entry_end)?;

        let reconstructed_shred_end_idx = if entry_end
            == shred_ranges
                .iter()
                .find(|range| range.shred_index == entry_byte_placement.end_shred_idx)
                .map(|range| range.serialized_offset_end_exclusive)
                .unwrap_or(entry_end.saturating_sub(1))
        {
            i64::from(entry_byte_placement.end_shred_idx)
        } else {
            -1
        };

        entry_placements.insert(
            entry.entry_index,
            EntryPlacement {
                serialized_offset_start: entry_start,
                serialized_offset_end_exclusive: entry_end,
                start_shred_idx: entry_byte_placement.start_shred_idx,
                end_shred_idx: entry_byte_placement.end_shred_idx,
                start_offset_in_shred: entry_byte_placement.start_offset_in_shred,
                end_offset_in_shred_exclusive: entry_byte_placement.end_offset_in_shred_exclusive,
                reconstructed_shred_end_idx,
            },
        );

        let mut next_transaction_offset = entry_start + entry.entry_prefix_len;
        for (transaction_slot_index, transaction_serialized_len) in entry
            .transaction_slot_indexes
            .iter()
            .copied()
            .zip(entry.transaction_serialized_lens.iter().copied())
        {
            let transaction_start = next_transaction_offset;
            let transaction_end = transaction_start + transaction_serialized_len;
            let transaction_byte_placement =
                locate_byte_placement(&shred_ranges, transaction_start, transaction_end)?;

            transaction_placements.insert(
                transaction_slot_index,
                TransactionPlacement {
                    entry_index: entry.entry_index,
                    serialized_offset_start: transaction_start,
                    serialized_offset_end_exclusive: transaction_end,
                    start_shred_idx: transaction_byte_placement.start_shred_idx,
                    end_shred_idx: transaction_byte_placement.end_shred_idx,
                    start_offset_in_shred: transaction_byte_placement.start_offset_in_shred,
                    end_offset_in_shred_exclusive: transaction_byte_placement
                        .end_offset_in_shred_exclusive,
                },
            );
            next_transaction_offset = transaction_end;
        }

        next_serialized_offset = entry_end;
    }

    Ok(Reconstruction {
        reconstructed_shred_count: data_shreds.len(),
        entry_placements,
        transaction_placements,
    })
}

fn build_shred_ranges(
    data_shreds: &[solana_ledger::shred::Shred],
    serialized_len: usize,
) -> Result<Vec<ShredByteRange>, String> {
    let mut next_serialized_offset = 0usize;
    let mut ranges = Vec::with_capacity(data_shreds.len());

    for shred in data_shreds {
        let data_len = extract_data_len(shred.payload().as_ref()).map_err(|err| {
            format!(
                "failed to extract reconstructed data from shred {}: {err}",
                shred.index()
            )
        })?;
        let range = ShredByteRange {
            shred_index: shred.index(),
            serialized_offset_start: next_serialized_offset,
            serialized_offset_end_exclusive: next_serialized_offset + data_len,
        };
        next_serialized_offset = range.serialized_offset_end_exclusive;
        ranges.push(range);
    }

    if next_serialized_offset != serialized_len {
        return Err(format!(
            "reconstructed shreds covered {next_serialized_offset} bytes but serialized slot had {serialized_len}"
        ));
    }

    Ok(ranges)
}

fn extract_data_len(shred_payload: &[u8]) -> Result<usize, String> {
    let size_bytes = shred_payload
        .get(86..88)
        .ok_or_else(|| "payload too short to contain data shred size".to_string())?;
    let size = u16::from_le_bytes([size_bytes[0], size_bytes[1]]) as usize;
    if size < SIZE_OF_DATA_SHRED_HEADERS {
        return Err(format!(
            "data shred size {size} was smaller than header size {SIZE_OF_DATA_SHRED_HEADERS}"
        ));
    }
    if size > shred_payload.len() {
        return Err(format!(
            "data shred size {size} exceeded payload length {}",
            shred_payload.len()
        ));
    }
    Ok(size - SIZE_OF_DATA_SHRED_HEADERS)
}

fn locate_byte_placement(
    shred_ranges: &[ShredByteRange],
    serialized_offset_start: usize,
    serialized_offset_end_exclusive: usize,
) -> Result<BytePlacement, String> {
    if serialized_offset_start >= serialized_offset_end_exclusive {
        return Err("cannot place an empty serialized range into shreds".to_string());
    }

    let start_range = shred_ranges
        .iter()
        .find(|range| {
            range.serialized_offset_start <= serialized_offset_start
                && serialized_offset_start < range.serialized_offset_end_exclusive
        })
        .ok_or_else(|| {
            format!("could not place serialized offset {serialized_offset_start} into any shred")
        })?;
    let end_byte = serialized_offset_end_exclusive - 1;
    let end_range = shred_ranges
        .iter()
        .find(|range| {
            range.serialized_offset_start <= end_byte
                && end_byte < range.serialized_offset_end_exclusive
        })
        .ok_or_else(|| format!("could not place serialized offset {end_byte} into any shred"))?;

    Ok(BytePlacement {
        start_shred_idx: start_range.shred_index,
        end_shred_idx: end_range.shred_index,
        start_offset_in_shred: serialized_offset_start - start_range.serialized_offset_start,
        end_offset_in_shred_exclusive: serialized_offset_end_exclusive
            - end_range.serialized_offset_start,
    })
}

fn merge_entries(
    pending_entries: &HashMap<usize, PendingEntry>,
    shredding_by_entry_end_idx: &HashMap<i64, ShredDumpEntry>,
    entry_placements: Option<&HashMap<usize, EntryPlacement>>,
) -> Vec<EntryDumpEntry> {
    let mut entries = pending_entries.values().cloned().collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.entry_index);

    entries
        .into_iter()
        .map(|entry| {
            let shredding = shredding_by_entry_end_idx
                .get(&(entry.entry_index as i64))
                .cloned();
            let placement =
                entry_placements.and_then(|placements| placements.get(&entry.entry_index));
            let raw_shred_end_idx_matches_reconstruction = match (&shredding, placement) {
                (Some(raw), Some(placement)) => {
                    Some(raw.shred_end_idx == placement.reconstructed_shred_end_idx)
                }
                _ => None,
            };

            EntryDumpEntry {
                entry_index: entry.entry_index,
                transaction_start_idx: entry.transaction_start_idx,
                transaction_end_idx_exclusive: entry.transaction_end_idx_exclusive,
                transaction_count: entry
                    .transaction_end_idx_exclusive
                    .saturating_sub(entry.transaction_start_idx),
                num_hashes: entry.num_hashes,
                hash: entry.hash.to_string(),
                shredding,
                serialized_offset_start: placement
                    .map(|placement| placement.serialized_offset_start),
                serialized_offset_end_exclusive: placement
                    .map(|placement| placement.serialized_offset_end_exclusive),
                serialized_size: placement.map(|placement| {
                    placement
                        .serialized_offset_end_exclusive
                        .saturating_sub(placement.serialized_offset_start)
                }),
                start_shred_idx: placement.map(|placement| placement.start_shred_idx),
                end_shred_idx: placement.map(|placement| placement.end_shred_idx),
                start_offset_in_shred: placement.map(|placement| placement.start_offset_in_shred),
                end_offset_in_shred_exclusive: placement
                    .map(|placement| placement.end_offset_in_shred_exclusive),
                reconstructed_shred_end_idx: placement
                    .map(|placement| placement.reconstructed_shred_end_idx),
                raw_shred_end_idx_matches_reconstruction,
            }
        })
        .collect()
}

fn merge_transactions(
    pending_transactions: &HashMap<usize, PendingTransaction>,
    entries: &[EntryDumpEntry],
    transaction_placements: Option<&HashMap<usize, TransactionPlacement>>,
    executed_transaction_count: u64,
) -> Vec<TransactionDumpEntry> {
    let mut transactions = pending_transactions.values().cloned().collect::<Vec<_>>();
    transactions.sort_by_key(|transaction| transaction.transaction_slot_index);

    let mapped_len = usize::try_from(executed_transaction_count)
        .ok()
        .unwrap_or_else(|| {
            transactions
                .last()
                .map(|transaction| transaction.transaction_slot_index.saturating_add(1))
                .unwrap_or(0)
        });
    let mut entry_index_by_transaction = vec![None; mapped_len];
    for entry in entries {
        let end_idx = entry
            .transaction_end_idx_exclusive
            .min(entry_index_by_transaction.len());
        for slot_index in entry.transaction_start_idx.min(end_idx)..end_idx {
            entry_index_by_transaction[slot_index] = Some(entry.entry_index);
        }
    }

    transactions
        .into_iter()
        .map(|transaction| {
            let placement = transaction_placements
                .and_then(|placements| placements.get(&transaction.transaction_slot_index));
            let entry_index = placement
                .map(|placement| placement.entry_index)
                .or_else(|| {
                    entry_index_by_transaction
                        .get(transaction.transaction_slot_index)
                        .copied()
                        .flatten()
                });

            TransactionDumpEntry {
                transaction_slot_index: transaction.transaction_slot_index,
                signature: transaction.signature,
                is_vote: transaction.is_vote,
                entry_index,
                serialized_offset_start: placement
                    .map(|placement| placement.serialized_offset_start),
                serialized_offset_end_exclusive: placement
                    .map(|placement| placement.serialized_offset_end_exclusive),
                serialized_size: placement.map(|placement| {
                    placement
                        .serialized_offset_end_exclusive
                        .saturating_sub(placement.serialized_offset_start)
                }),
                start_shred_idx: placement.map(|placement| placement.start_shred_idx),
                end_shred_idx: placement.map(|placement| placement.end_shred_idx),
                start_offset_in_shred: placement.map(|placement| placement.start_offset_in_shred),
                end_offset_in_shred_exclusive: placement
                    .map(|placement| placement.end_offset_in_shred_exclusive),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn formats_block_records_as_json_lines() {
        let line = format_block_record(
            2,
            123,
            122,
            "blockhash111",
            "parenthash111",
            Some(456),
            Some(789),
            1,
            1,
            &[Shredding {
                entry_end_idx: 0,
                shred_end_idx: 0,
            }],
            &HashMap::from([(
                0usize,
                PendingEntry {
                    entry_index: 0,
                    transaction_start_idx: 0,
                    transaction_end_idx_exclusive: 1,
                    num_hashes: 1,
                    hash: SolanaHash::new_from_array([7u8; 32]),
                },
            )]),
            &HashMap::from([(
                0usize,
                PendingTransaction {
                    transaction_slot_index: 0,
                    signature: "sig0".to_string(),
                    is_vote: false,
                    transaction: VersionedTransaction::default(),
                },
            )]),
        )
        .unwrap();

        let parsed: serde_json::Value = serde_json::from_str(&line).unwrap();

        assert_eq!(parsed["event"], json!("block"));
        assert_eq!(parsed["metadata"]["shredding_count"], json!(1));
        assert_eq!(parsed["metadata"]["merged_entry_count"], json!(1));
        assert_eq!(parsed["metadata"]["merged_transaction_count"], json!(1));
        assert_eq!(parsed["metadata"]["reconstructed_shred_count"], json!(1));
        assert!(parsed["metadata"]["reconstruction_error"].is_null());
        assert_eq!(parsed["metadata"]["entries"][0]["entry_index"], json!(0));
        assert_eq!(
            parsed["metadata"]["entries"][0]["start_shred_idx"],
            json!(0)
        );
        assert_eq!(parsed["metadata"]["entries"][0]["end_shred_idx"], json!(0));
        assert_eq!(
            parsed["metadata"]["entries"][0]["raw_shred_end_idx_matches_reconstruction"],
            json!(true)
        );
        assert_eq!(
            parsed["metadata"]["transactions"][0]["entry_index"],
            json!(0)
        );
        assert_eq!(
            parsed["metadata"]["transactions"][0]["start_shred_idx"],
            json!(0)
        );
        assert_eq!(
            parsed["metadata"]["transactions"][0]["end_shred_idx"],
            json!(0)
        );
        assert!(
            parsed["metadata"]["transactions"][0]["serialized_size"]
                .as_u64()
                .unwrap()
                > 0
        );
    }

    #[test]
    fn formats_skipped_block_records_as_json_lines() {
        let line = format_skipped_block_record(4, 999).unwrap();

        let parsed: serde_json::Value = serde_json::from_str(&line).unwrap();

        assert_eq!(
            parsed,
            json!({
                "event": "possible_leader_skipped",
                "thread_id": 4,
                "slot": 999
            })
        );
    }
}
