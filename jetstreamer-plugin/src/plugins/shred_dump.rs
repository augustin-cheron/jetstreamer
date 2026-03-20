use std::{
    collections::HashMap,
    fmt::Write as _,
    io::{self, Write as _},
};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
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
    ProcessShredsStats, ReedSolomonCache, SIZE_OF_DATA_SHRED_HEADERS, Shred as LedgerShred,
    ShredType, Shredder,
};
use solana_transaction::versioned::VersionedTransaction;

use crate::{Plugin, PluginDb, PluginFuture};

static PENDING_BY_SLOT: Lazy<DashMap<u64, PendingSlotDump>> = Lazy::new(DashMap::new);

#[derive(Debug, Clone)]
/// Emits framed reconstructed shred payloads by default, with optional JSON debug views.
pub struct ShredDumpPlugin {
    debug_decode: bool,
    debug_view: bool,
    verbose: bool,
}

impl ShredDumpPlugin {
    /// Creates a new stdout shred dump plugin.
    pub const fn new() -> Self {
        Self {
            debug_decode: false,
            debug_view: false,
            verbose: false,
        }
    }

    /// Enables or disables decoder-verified shred debug output.
    pub const fn with_debug_decode(mut self, debug_decode: bool) -> Self {
        self.debug_decode = debug_decode;
        self
    }

    /// Switches the plugin from the default shred stream into JSON debug view.
    pub const fn with_debug_view(mut self, debug_view: bool) -> Self {
        self.debug_view = debug_view;
        self
    }

    /// Includes verbose CAR-derived block metadata in JSON debug view.
    pub const fn with_verbose(mut self, verbose: bool) -> Self {
        self.verbose = verbose;
        self
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
        _db: PluginDb,
        transaction: &'a TransactionData,
    ) -> PluginFuture<'a> {
        async move {
            let mut slot_dump = PENDING_BY_SLOT.entry(transaction.slot).or_default();
            slot_dump.transactions.insert(
                transaction.transaction_slot_index,
                PendingTransaction {
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
        _db: PluginDb,
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
        _db: PluginDb,
        block: &'a BlockData,
    ) -> PluginFuture<'a> {
        async move {
            let pending = Self::take_pending_slot(block.slot());
            print_block_record(
                thread_id,
                block,
                pending,
                self.debug_view,
                self.debug_decode,
                self.verbose,
            );
            Ok(())
        }
        .boxed()
    }

    #[inline(always)]
    fn on_exit(&self, _db: PluginDb) -> PluginFuture<'_> {
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
    transaction: VersionedTransaction,
}

struct DebugRecordContext<'a> {
    thread_id: usize,
    slot: u64,
    parent_slot: u64,
    blockhash: &'a str,
    parent_blockhash: &'a str,
    block_time: Option<i64>,
    block_height: Option<u64>,
    executed_transaction_count: u64,
    entry_count: u64,
    shredding: &'a [Shredding],
    pending_entries: &'a HashMap<usize, PendingEntry>,
    pending_transactions: &'a HashMap<usize, PendingTransaction>,
    debug_decode: bool,
    verbose: bool,
}

#[derive(Serialize)]
struct ShredDebugRecord {
    event: &'static str,
    thread_id: usize,
    slot: u64,
    reconstructed_shred_count: Option<usize>,
    reconstructed_code_shred_count: Option<usize>,
    reconstructed_total_shred_count: Option<usize>,
    raw_shred_count_estimate: Option<i64>,
    reconstruction_error: Option<String>,
    reconstructed_shreds: Vec<ReconstructedShredDumpEntry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    car_view: Option<CarDebugView>,
}

#[derive(Serialize)]
struct CarDebugView {
    parent_slot: u64,
    blockhash: String,
    parent_blockhash: String,
    block_time: Option<i64>,
    block_height: Option<u64>,
    executed_transaction_count: u64,
    entry_count: u64,
    shredding: Vec<ShredDumpEntry>,
    entries: Vec<EntryDumpEntry>,
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
struct SkippedBlockRecord {
    event: &'static str,
    thread_id: usize,
    slot: u64,
}

struct Reconstruction {
    reconstructed_shred_count: usize,
    reconstructed_code_shred_count: usize,
    reconstructed_shreds: Vec<ReconstructedShredDumpEntry>,
    reconstructed_shred_payloads: Vec<Vec<u8>>,
    entry_placements: HashMap<usize, EntryPlacement>,
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
    serialized_offset_start: usize,
    serialized_offset_end_exclusive: usize,
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

#[derive(Serialize, Clone)]
struct ReconstructedShredDumpEntry {
    shred_type: &'static str,
    slot: u64,
    index: u32,
    version: u16,
    fec_set_index: u32,
    erasure_shard_index: usize,
    proof_size: u8,
    retransmitter_signed: bool,
    payload_size: usize,
    payload_base64: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    payload_clear: Option<String>,
    serialized_offset_start: Option<usize>,
    serialized_offset_end_exclusive: Option<usize>,
    entry_indexes: Vec<usize>,
    transaction_slot_indexes: Vec<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    debug_decoded: Option<DecodedShredDebugEntry>,
    data: Option<ReconstructedDataShredDumpEntry>,
    code: Option<ReconstructedCodeShredDumpEntry>,
}

#[derive(Serialize, Clone)]
struct DecodedShredDebugEntry {
    roundtrip_decode_ok: bool,
    payload_roundtrip_matches: bool,
    sanitize_ok: bool,
    sanitize_error: Option<String>,
    decoded_shred_type: Option<&'static str>,
    decoded_slot: Option<u64>,
    decoded_index: Option<u32>,
    decoded_version: Option<u16>,
    decoded_fec_set_index: Option<u32>,
    decoded_parent_slot: Option<u64>,
    decoded_last_in_slot: Option<bool>,
    decoded_data_complete: Option<bool>,
    decoded_merkle_root: Option<String>,
    decoded_chained_merkle_root: Option<String>,
    decode_error: Option<String>,
}

#[derive(Serialize, Clone)]
struct ReconstructedDataShredDumpEntry {
    parent_offset: u16,
    parent_slot: u64,
    flags: u8,
    reference_tick: u8,
    data_complete: bool,
    last_in_slot: bool,
    size: u16,
    data_len: usize,
}

#[derive(Serialize, Clone)]
struct ReconstructedCodeShredDumpEntry {
    num_data_shreds: u16,
    num_coding_shreds: u16,
    position: u16,
    covered_data_shred_indexes: Vec<u32>,
}

fn print_block_record(
    thread_id: usize,
    block: &BlockData,
    pending: PendingSlotDump,
    debug_view: bool,
    debug_decode: bool,
    verbose: bool,
) {
    if !debug_view {
        if let Err(err) = write_shred_stream_record(block, pending) {
            eprintln!("failed to write shred stream record: {err}");
        }
        return;
    }

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
        } => {
            let blockhash = blockhash.to_string();
            let parent_blockhash = parent_blockhash.to_string();
            format_debug_record(&DebugRecordContext {
                thread_id,
                slot: *slot,
                parent_slot: *parent_slot,
                blockhash: &blockhash,
                parent_blockhash: &parent_blockhash,
                block_time: *block_time,
                block_height: *block_height,
                executed_transaction_count: *executed_transaction_count,
                entry_count: *entry_count,
                shredding,
                pending_entries: &pending.entries,
                pending_transactions: &pending.transactions,
                debug_decode,
                verbose,
            })
        }
        BlockData::PossibleLeaderSkipped { slot } => {
            ShredDumpPlugin::clear_pending_slot(*slot);
            format_skipped_block_record(thread_id, *slot)
        }
    };

    match serialized {
        Ok(line) => println!("{line}"),
        Err(err) => eprintln!("failed to serialize shred dump debug record: {err}"),
    }
}

fn write_shred_stream_record(block: &BlockData, pending: PendingSlotDump) -> Result<(), String> {
    let payloads = match block {
        BlockData::Block {
            slot, parent_slot, ..
        } => {
            reconstruct_slot_layout(
                *slot,
                *parent_slot,
                &pending.entries,
                &pending.transactions,
                false,
            )?
            .reconstructed_shred_payloads
        }
        BlockData::PossibleLeaderSkipped { slot } => {
            ShredDumpPlugin::clear_pending_slot(*slot);
            return Ok(());
        }
    };

    let mut stdout = io::stdout().lock();
    let framed = encode_shred_stream(&payloads)?;
    stdout
        .write_all(&framed)
        .map_err(|err| format!("failed to write shred stream payloads to stdout: {err}"))?;
    stdout
        .flush()
        .map_err(|err| format!("failed to flush shred stream stdout: {err}"))?;
    Ok(())
}

fn format_debug_record(input: &DebugRecordContext<'_>) -> Result<String, serde_json::Error> {
    let resolved_shredding = resolve_shredding(input.shredding);
    let shredding_by_entry_end_idx = resolved_shredding
        .iter()
        .cloned()
        .map(|entry| (entry.entry_end_idx, entry))
        .collect::<HashMap<_, _>>();

    let reconstruction = match reconstruct_slot_layout(
        input.slot,
        input.parent_slot,
        input.pending_entries,
        input.pending_transactions,
        input.debug_decode,
    ) {
        Ok(layout) => (Some(layout), None),
        Err(err) => (None, Some(err)),
    };
    let reconstructed_shred_count = reconstruction
        .0
        .as_ref()
        .map(|layout| layout.reconstructed_shred_count);
    let reconstructed_code_shred_count = reconstruction
        .0
        .as_ref()
        .map(|layout| layout.reconstructed_code_shred_count);
    let reconstructed_total_shred_count = reconstruction.0.as_ref().map(|layout| {
        layout
            .reconstructed_shred_count
            .saturating_add(layout.reconstructed_code_shred_count)
    });
    let reconstructed_shreds = reconstruction
        .0
        .as_ref()
        .map(|layout| layout.reconstructed_shreds.clone())
        .unwrap_or_default();
    let entry_placements = reconstruction
        .0
        .as_ref()
        .map(|layout| &layout.entry_placements);
    let car_view = input.verbose.then(|| CarDebugView {
        parent_slot: input.parent_slot,
        blockhash: input.blockhash.to_owned(),
        parent_blockhash: input.parent_blockhash.to_owned(),
        block_time: input.block_time,
        block_height: input.block_height,
        executed_transaction_count: input.executed_transaction_count,
        entry_count: input.entry_count,
        shredding: resolved_shredding.clone(),
        entries: merge_entries(
            input.pending_entries,
            &shredding_by_entry_end_idx,
            entry_placements,
        ),
    });

    let record = ShredDebugRecord {
        event: "reconstructed_shreds",
        thread_id: input.thread_id,
        slot: input.slot,
        reconstructed_shred_count,
        reconstructed_code_shred_count,
        reconstructed_total_shred_count,
        raw_shred_count_estimate: raw_shred_count_estimate(input.shredding),
        reconstruction_error: reconstruction.1,
        reconstructed_shreds,
        car_view,
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
    debug_decode: bool,
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
    let (mut data_shreds, mut coding_shreds) = shredder.entries_to_merkle_shreds_for_tests(
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
    coding_shreds.sort_by_key(|shred| shred.index());

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
                    serialized_offset_start: transaction_start,
                    serialized_offset_end_exclusive: transaction_end,
                },
            );
            next_transaction_offset = transaction_end;
        }

        next_serialized_offset = entry_end;
    }

    let reconstructed_shreds = build_reconstructed_shred_entries(
        &data_shreds,
        &coding_shreds,
        &shred_ranges,
        &entry_placements,
        &transaction_placements,
        debug_decode,
    )?;
    let reconstructed_shred_payloads =
        collect_reconstructed_shred_payloads(&data_shreds, &coding_shreds);

    Ok(Reconstruction {
        reconstructed_shred_count: data_shreds.len(),
        reconstructed_code_shred_count: coding_shreds.len(),
        reconstructed_shreds,
        reconstructed_shred_payloads,
        entry_placements,
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

fn build_reconstructed_shred_entries(
    data_shreds: &[solana_ledger::shred::Shred],
    coding_shreds: &[solana_ledger::shred::Shred],
    shred_ranges: &[ShredByteRange],
    entry_placements: &HashMap<usize, EntryPlacement>,
    transaction_placements: &HashMap<usize, TransactionPlacement>,
    debug_decode: bool,
) -> Result<Vec<ReconstructedShredDumpEntry>, String> {
    let serialized_range_by_shred = shred_ranges
        .iter()
        .map(|range| {
            (
                range.shred_index,
                (
                    range.serialized_offset_start,
                    range.serialized_offset_end_exclusive,
                ),
            )
        })
        .collect::<HashMap<_, _>>();

    let mut entry_indexes_by_data_shred: HashMap<u32, Vec<usize>> = HashMap::new();
    for (&entry_index, placement) in entry_placements {
        for range in shred_ranges.iter().filter(|range| {
            ranges_overlap(
                placement.serialized_offset_start,
                placement.serialized_offset_end_exclusive,
                range.serialized_offset_start,
                range.serialized_offset_end_exclusive,
            )
        }) {
            entry_indexes_by_data_shred
                .entry(range.shred_index)
                .or_default()
                .push(entry_index);
        }
    }

    let mut transaction_indexes_by_data_shred: HashMap<u32, Vec<usize>> = HashMap::new();
    for (&transaction_slot_index, placement) in transaction_placements {
        for range in shred_ranges.iter().filter(|range| {
            ranges_overlap(
                placement.serialized_offset_start,
                placement.serialized_offset_end_exclusive,
                range.serialized_offset_start,
                range.serialized_offset_end_exclusive,
            )
        }) {
            transaction_indexes_by_data_shred
                .entry(range.shred_index)
                .or_default()
                .push(transaction_slot_index);
        }
    }

    for indexes in entry_indexes_by_data_shred.values_mut() {
        indexes.sort_unstable();
        indexes.dedup();
    }
    for indexes in transaction_indexes_by_data_shred.values_mut() {
        indexes.sort_unstable();
        indexes.dedup();
    }

    let mut reconstructed_shreds =
        Vec::with_capacity(data_shreds.len().saturating_add(coding_shreds.len()));

    for shred in data_shreds {
        let variant = decode_variant(shred.payload().as_ref())?;
        let payload = shred.payload().as_ref();
        let flags = extract_data_flags(payload)?;
        let size = extract_data_size(payload)?;
        let parent_offset = extract_data_parent_offset(payload)?;
        reconstructed_shreds.push(ReconstructedShredDumpEntry {
            shred_type: "data",
            slot: shred.slot(),
            index: shred.index(),
            version: shred.version(),
            fec_set_index: shred.fec_set_index(),
            erasure_shard_index: usize::try_from(
                shred.index().saturating_sub(shred.fec_set_index()),
            )
            .unwrap_or(usize::MAX),
            proof_size: variant.proof_size,
            retransmitter_signed: variant.retransmitter_signed,
            payload_size: payload.len(),
            payload_base64: BASE64_STANDARD.encode(payload),
            payload_clear: debug_decode.then(|| encode_payload_clear(payload)),
            serialized_offset_start: serialized_range_by_shred
                .get(&shred.index())
                .map(|(start, _)| *start),
            serialized_offset_end_exclusive: serialized_range_by_shred
                .get(&shred.index())
                .map(|(_, end)| *end),
            entry_indexes: entry_indexes_by_data_shred
                .get(&shred.index())
                .cloned()
                .unwrap_or_default(),
            transaction_slot_indexes: transaction_indexes_by_data_shred
                .get(&shred.index())
                .cloned()
                .unwrap_or_default(),
            debug_decoded: debug_decode.then(|| build_decoded_shred_debug(payload)),
            data: Some(ReconstructedDataShredDumpEntry {
                parent_offset,
                parent_slot: shred.parent().map_err(|err| {
                    format!(
                        "failed to read parent slot from reconstructed data shred {}: {err}",
                        shred.index()
                    )
                })?,
                flags,
                reference_tick: flags & 0b0011_1111,
                data_complete: shred.data_complete(),
                last_in_slot: shred.last_in_slot(),
                size,
                data_len: usize::from(size).saturating_sub(SIZE_OF_DATA_SHRED_HEADERS),
            }),
            code: None,
        });
    }

    for shred in coding_shreds {
        let variant = decode_variant(shred.payload().as_ref())?;
        let payload = shred.payload().as_ref();
        let coding_header = extract_coding_header(payload)?;
        let covered_data_shred_indexes = (0..u32::from(coding_header.num_data_shreds))
            .map(|offset| shred.fec_set_index().saturating_add(offset))
            .collect::<Vec<_>>();
        let mut entry_indexes = covered_data_shred_indexes
            .iter()
            .filter_map(|index| entry_indexes_by_data_shred.get(index))
            .flat_map(|indexes| indexes.iter().copied())
            .collect::<Vec<_>>();
        entry_indexes.sort_unstable();
        entry_indexes.dedup();
        let mut transaction_slot_indexes = covered_data_shred_indexes
            .iter()
            .filter_map(|index| transaction_indexes_by_data_shred.get(index))
            .flat_map(|indexes| indexes.iter().copied())
            .collect::<Vec<_>>();
        transaction_slot_indexes.sort_unstable();
        transaction_slot_indexes.dedup();
        reconstructed_shreds.push(ReconstructedShredDumpEntry {
            shred_type: "code",
            slot: shred.slot(),
            index: shred.index(),
            version: shred.version(),
            fec_set_index: shred.fec_set_index(),
            erasure_shard_index: usize::from(coding_header.num_data_shreds)
                .saturating_add(usize::from(coding_header.position)),
            proof_size: variant.proof_size,
            retransmitter_signed: variant.retransmitter_signed,
            payload_size: payload.len(),
            payload_base64: BASE64_STANDARD.encode(payload),
            payload_clear: debug_decode.then(|| encode_payload_clear(payload)),
            serialized_offset_start: None,
            serialized_offset_end_exclusive: None,
            entry_indexes,
            transaction_slot_indexes,
            debug_decoded: debug_decode.then(|| build_decoded_shred_debug(payload)),
            data: None,
            code: Some(ReconstructedCodeShredDumpEntry {
                num_data_shreds: coding_header.num_data_shreds,
                num_coding_shreds: coding_header.num_coding_shreds,
                position: coding_header.position,
                covered_data_shred_indexes,
            }),
        });
    }

    reconstructed_shreds.sort_by_key(|shred| {
        (
            shred.slot,
            shred.fec_set_index,
            match shred.shred_type {
                "data" => 0u8,
                _ => 1u8,
            },
            shred.index,
        )
    });

    Ok(reconstructed_shreds)
}

fn build_decoded_shred_debug(payload: &[u8]) -> DecodedShredDebugEntry {
    match LedgerShred::new_from_serialized_shred(payload.to_vec()) {
        Ok(decoded) => {
            let sanitize = decoded.sanitize();
            let sanitize_ok = sanitize.is_ok();
            let sanitize_error = sanitize.err().map(|err| err.to_string());
            DecodedShredDebugEntry {
                roundtrip_decode_ok: true,
                payload_roundtrip_matches: decoded.payload().as_ref() == payload,
                sanitize_ok,
                sanitize_error,
                decoded_shred_type: Some(match decoded.shred_type() {
                    ShredType::Data => "data",
                    ShredType::Code => "code",
                }),
                decoded_slot: Some(decoded.slot()),
                decoded_index: Some(decoded.index()),
                decoded_version: Some(decoded.version()),
                decoded_fec_set_index: Some(decoded.fec_set_index()),
                decoded_parent_slot: decoded.parent().ok(),
                decoded_last_in_slot: decoded.is_data().then_some(decoded.last_in_slot()),
                decoded_data_complete: decoded.is_data().then_some(decoded.data_complete()),
                decoded_merkle_root: decoded.merkle_root().ok().map(|hash| hash.to_string()),
                decoded_chained_merkle_root: decoded
                    .chained_merkle_root()
                    .ok()
                    .map(|hash| hash.to_string()),
                decode_error: None,
            }
        }
        Err(err) => DecodedShredDebugEntry {
            roundtrip_decode_ok: false,
            payload_roundtrip_matches: false,
            sanitize_ok: false,
            sanitize_error: None,
            decoded_shred_type: None,
            decoded_slot: None,
            decoded_index: None,
            decoded_version: None,
            decoded_fec_set_index: None,
            decoded_parent_slot: None,
            decoded_last_in_slot: None,
            decoded_data_complete: None,
            decoded_merkle_root: None,
            decoded_chained_merkle_root: None,
            decode_error: Some(err.to_string()),
        },
    }
}

fn collect_reconstructed_shred_payloads(
    data_shreds: &[solana_ledger::shred::Shred],
    coding_shreds: &[solana_ledger::shred::Shred],
) -> Vec<Vec<u8>> {
    let mut payloads = data_shreds
        .iter()
        .map(|shred| {
            (
                shred.fec_set_index(),
                0u8,
                shred.index(),
                shred.payload().as_ref().to_vec(),
            )
        })
        .chain(coding_shreds.iter().map(|shred| {
            (
                shred.fec_set_index(),
                1u8,
                shred.index(),
                shred.payload().as_ref().to_vec(),
            )
        }))
        .collect::<Vec<_>>();
    payloads.sort_by_key(|(fec_set_index, shred_type_order, index, _)| {
        (*fec_set_index, *shred_type_order, *index)
    });
    payloads
        .into_iter()
        .map(|(_, _, _, payload)| payload)
        .collect()
}

fn encode_payload_clear(payload: &[u8]) -> String {
    let mut clear = String::with_capacity(payload.len().saturating_mul(2));
    for byte in payload {
        write!(clear, "{byte:02x}").expect("writing to String cannot fail");
    }
    clear
}

fn encode_shred_stream(payloads: &[Vec<u8>]) -> Result<Vec<u8>, String> {
    let total_len = payloads.iter().try_fold(0usize, |acc, payload| {
        let frame_len = 4usize
            .checked_add(payload.len())
            .ok_or_else(|| "shred stream frame length overflowed usize".to_string())?;
        acc.checked_add(frame_len)
            .ok_or_else(|| "total shred stream length overflowed usize".to_string())
    })?;
    let mut encoded = Vec::with_capacity(total_len);
    for payload in payloads {
        let payload_len = u32::try_from(payload.len())
            .map_err(|_| format!("shred payload length {} exceeded u32::MAX", payload.len()))?;
        encoded.extend_from_slice(&payload_len.to_be_bytes());
        encoded.extend_from_slice(payload);
    }
    Ok(encoded)
}

fn ranges_overlap(
    left_start: usize,
    left_end_exclusive: usize,
    right_start: usize,
    right_end_exclusive: usize,
) -> bool {
    left_start < right_end_exclusive && right_start < left_end_exclusive
}

struct DecodedVariant {
    proof_size: u8,
    retransmitter_signed: bool,
}

fn decode_variant(shred_payload: &[u8]) -> Result<DecodedVariant, String> {
    let variant = *shred_payload
        .get(64)
        .ok_or_else(|| "payload too short to contain shred variant".to_string())?;
    let proof_size = variant & 0x0F;
    let retransmitter_signed = match variant & 0xF0 {
        0x60 | 0x90 => false,
        0x70 | 0xB0 => true,
        other => {
            return Err(format!(
                "unsupported reconstructed shred variant byte 0x{other:02x}"
            ));
        }
    };
    Ok(DecodedVariant {
        proof_size,
        retransmitter_signed,
    })
}

fn extract_data_parent_offset(shred_payload: &[u8]) -> Result<u16, String> {
    extract_u16(shred_payload, 83, "data parent offset")
}

fn extract_data_flags(shred_payload: &[u8]) -> Result<u8, String> {
    shred_payload
        .get(85)
        .copied()
        .ok_or_else(|| "payload too short to contain data shred flags".to_string())
}

fn extract_data_size(shred_payload: &[u8]) -> Result<u16, String> {
    extract_u16(shred_payload, 86, "data shred size")
}

struct CodingHeaderFields {
    num_data_shreds: u16,
    num_coding_shreds: u16,
    position: u16,
}

fn extract_coding_header(shred_payload: &[u8]) -> Result<CodingHeaderFields, String> {
    Ok(CodingHeaderFields {
        num_data_shreds: extract_u16(shred_payload, 83, "coding num_data_shreds")?,
        num_coding_shreds: extract_u16(shred_payload, 85, "coding num_coding_shreds")?,
        position: extract_u16(shred_payload, 87, "coding position")?,
    })
}

fn extract_u16(shred_payload: &[u8], offset: usize, field_name: &str) -> Result<u16, String> {
    let bytes = shred_payload
        .get(offset..offset + 2)
        .ok_or_else(|| format!("payload too short to contain {field_name}"))?;
    Ok(u16::from_le_bytes([bytes[0], bytes[1]]))
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn formats_debug_records_as_json_lines() {
        let shredding = [Shredding {
            entry_end_idx: 0,
            shred_end_idx: 0,
        }];
        let pending_entries = HashMap::from([(
            0usize,
            PendingEntry {
                entry_index: 0,
                transaction_start_idx: 0,
                transaction_end_idx_exclusive: 1,
                num_hashes: 1,
                hash: SolanaHash::new_from_array([7u8; 32]),
            },
        )]);
        let pending_transactions = HashMap::from([(
            0usize,
            PendingTransaction {
                transaction: VersionedTransaction::default(),
            },
        )]);
        let line = format_debug_record(&DebugRecordContext {
            thread_id: 2,
            slot: 123,
            parent_slot: 122,
            blockhash: "blockhash111",
            parent_blockhash: "parenthash111",
            block_time: Some(456),
            block_height: Some(789),
            executed_transaction_count: 1,
            entry_count: 1,
            shredding: &shredding,
            pending_entries: &pending_entries,
            pending_transactions: &pending_transactions,
            debug_decode: false,
            verbose: false,
        })
        .unwrap();

        let parsed: serde_json::Value = serde_json::from_str(&line).unwrap();

        assert_eq!(parsed["event"], json!("reconstructed_shreds"));
        assert_eq!(parsed["slot"], json!(123));
        assert_eq!(parsed["reconstructed_shred_count"], json!(1));
        assert!(
            parsed["reconstructed_total_shred_count"].as_u64().unwrap()
                >= parsed["reconstructed_shred_count"].as_u64().unwrap()
        );
        assert!(parsed["reconstruction_error"].is_null());
        assert_eq!(
            parsed["reconstructed_shreds"].as_array().unwrap().len(),
            parsed["reconstructed_total_shred_count"].as_u64().unwrap() as usize
        );
        assert!(
            parsed["reconstructed_shreds"]
                .as_array()
                .unwrap()
                .iter()
                .any(|shred| shred["shred_type"] == json!("data")
                    && shred["payload_base64"].as_str().is_some())
        );
        assert!(
            parsed["reconstructed_shreds"][0]
                .get("debug_decoded")
                .is_none()
        );
        assert!(
            parsed["reconstructed_shreds"][0]
                .get("payload_clear")
                .is_none()
        );
        assert!(parsed.get("car_view").is_none());
    }

    #[test]
    fn formats_decoded_shred_debug_when_requested() {
        let shredding = [Shredding {
            entry_end_idx: 0,
            shred_end_idx: 0,
        }];
        let pending_entries = HashMap::from([(
            0usize,
            PendingEntry {
                entry_index: 0,
                transaction_start_idx: 0,
                transaction_end_idx_exclusive: 1,
                num_hashes: 1,
                hash: SolanaHash::new_from_array([7u8; 32]),
            },
        )]);
        let pending_transactions = HashMap::from([(
            0usize,
            PendingTransaction {
                transaction: VersionedTransaction::default(),
            },
        )]);
        let line = format_debug_record(&DebugRecordContext {
            thread_id: 2,
            slot: 123,
            parent_slot: 122,
            blockhash: "blockhash111",
            parent_blockhash: "parenthash111",
            block_time: Some(456),
            block_height: Some(789),
            executed_transaction_count: 1,
            entry_count: 1,
            shredding: &shredding,
            pending_entries: &pending_entries,
            pending_transactions: &pending_transactions,
            debug_decode: true,
            verbose: false,
        })
        .unwrap();

        let parsed: serde_json::Value = serde_json::from_str(&line).unwrap();
        let debug = &parsed["reconstructed_shreds"][0]["debug_decoded"];
        let payload_clear = &parsed["reconstructed_shreds"][0]["payload_clear"];

        assert_eq!(debug["roundtrip_decode_ok"], json!(true));
        assert_eq!(debug["payload_roundtrip_matches"], json!(true));
        assert_eq!(debug["sanitize_ok"], json!(true));
        assert!(debug["decoded_slot"].as_u64().is_some());
        assert!(payload_clear.as_str().is_some());
        assert!(!payload_clear.as_str().unwrap().is_empty());
    }

    #[test]
    fn formats_verbose_debug_records_with_car_view() {
        let shredding = [Shredding {
            entry_end_idx: 0,
            shred_end_idx: 0,
        }];
        let pending_entries = HashMap::from([(
            0usize,
            PendingEntry {
                entry_index: 0,
                transaction_start_idx: 0,
                transaction_end_idx_exclusive: 1,
                num_hashes: 1,
                hash: SolanaHash::new_from_array([7u8; 32]),
            },
        )]);
        let pending_transactions = HashMap::from([(
            0usize,
            PendingTransaction {
                transaction: VersionedTransaction::default(),
            },
        )]);
        let line = format_debug_record(&DebugRecordContext {
            thread_id: 2,
            slot: 123,
            parent_slot: 122,
            blockhash: "blockhash111",
            parent_blockhash: "parenthash111",
            block_time: Some(456),
            block_height: Some(789),
            executed_transaction_count: 1,
            entry_count: 1,
            shredding: &shredding,
            pending_entries: &pending_entries,
            pending_transactions: &pending_transactions,
            debug_decode: false,
            verbose: true,
        })
        .unwrap();

        let parsed: serde_json::Value = serde_json::from_str(&line).unwrap();

        assert_eq!(parsed["car_view"]["parent_slot"], json!(122));
        assert_eq!(parsed["car_view"]["blockhash"], json!("blockhash111"));
        assert_eq!(
            parsed["car_view"]["shredding"][0]["entry_end_idx"],
            json!(0)
        );
        assert_eq!(parsed["car_view"]["entries"][0]["entry_index"], json!(0));
        assert_eq!(
            parsed["car_view"]["entries"][0]["start_shred_idx"],
            json!(0)
        );
    }

    #[test]
    fn encodes_shred_stream_with_big_endian_length_prefixes() {
        let encoded = encode_shred_stream(&[vec![1u8, 2, 3], vec![4u8]]).unwrap();

        assert_eq!(&encoded[0..4], &3u32.to_be_bytes());
        assert_eq!(&encoded[4..7], &[1u8, 2, 3]);
        assert_eq!(&encoded[7..11], &1u32.to_be_bytes());
        assert_eq!(&encoded[11..12], &[4u8]);
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
