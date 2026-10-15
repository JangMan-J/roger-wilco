import type { Outcome } from './protocol/index.ts';

/** Result of asking whether one message is persisted in a session's current store. */
export type EvidenceLookup<Evidence> =
  | { state: 'persisted'; evidence: Evidence }
  | { state: 'absent' }
  /** The store does not exist yet; nothing in it can be persisted, so delivery may proceed. */
  | { state: 'store_missing' };

/**
 * The delivery-evidence seam: "the message is persisted; here is the evidence."
 * Only exact, durable evidence read from the current store may be returned as
 * 'persisted'. Anything indeterminate (concurrent change, unreadable store)
 * must throw so the caller retries instead of acknowledging.
 * Every harness adapter supplies one implementation; the core never interprets it.
 */
export interface EvidenceSource<Evidence> {
  /** The session's current store identity; undefined while the session has none. */
  location(): string | undefined;
  /** Look up exact durable evidence that `messageId` is persisted at `location`. An adapter may pass the full expected message so the lookup can reject a record that merely names the ID. */
  lookup(location: string | undefined, messageId: string, expected?: unknown): Promise<EvidenceLookup<Evidence>>;
  /** True only when `evidence` was taken from `location`. */
  locatedAt(evidence: Evidence, location: string): boolean;
  /** Present the evidence to the host as the delivery acknowledgement, translated to the core's DeliveryEvidence. */
  acknowledge(evidence: Evidence): Promise<Outcome<null>>;
}
