import type { ExtensionAPI } from '@earendil-works/pi-coding-agent';
import { createPiAdapter } from './adapter.ts';
import { loadPiConfig } from './config.ts';
import { connectHost, requestCoordinator } from './host.ts';

export { loadPiConfig };

/** Pi extension entry: wire the Pi adapter to its config and core clients. */
export default function rogerWilco(pi: ExtensionAPI): void {
  createPiAdapter(pi, { loadConfig: () => loadPiConfig(), connectHost, request: requestCoordinator });
}
