/** Claude Code adapter entry points. Compose `createClaudeAdapter` with a harness, or run the CLI harness from `src/bin/claude.ts`. */
export { createClaudeAdapter, type ClaudeAdapter, type ClaudeAdapterOptions, type ClaudeHarness } from './adapter.ts';
export { loadClaudeConfig, type ClaudeConfig } from './config.ts';
export { claudeSessionAddress } from './identity.ts';
export { findClaudeTranscript, isDeliveredRecord, claudeReceiptEvidence, claudeTranscriptEvidence, type ClaudeReceipt } from './transcript.ts';
export { createClaudeTools, type BoundSession, type CoreAccess } from './tools.ts';
